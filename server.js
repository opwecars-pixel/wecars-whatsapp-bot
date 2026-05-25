const express = require("express");
const OpenAI = require("openai");
const axios = require("axios");
const { Pool } = require("pg");
const cloudinary = require("cloudinary").v2;

const app = express();

app.use(express.urlencoded({ extended: true }));
app.use(express.json());

// ── CORS para weMarket Zone ───────────────────────────────────────────────────
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Content-Type");
  res.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

// ── Cloudinary ────────────────────────────────────────────────────────────────
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key:    process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
  secure:     true,
});

// Descarga foto de Twilio (requiere auth) y la sube a Cloudinary
async function subirFotoCloudinary(twilioUrl, index, telefono) {
  try {
    // 1. Descarga autenticada desde Twilio
    const response = await axios.get(twilioUrl, {
      responseType: "arraybuffer",
      auth: {
        username: process.env.TWILIO_ACCOUNT_SID,
        password: process.env.TWILIO_AUTH_TOKEN,
      },
      timeout: 15000,
    });

    const buffer = Buffer.from(response.data);
    const mimeType = response.headers["content-type"] || "image/jpeg";

    // 2. Sube a Cloudinary como stream desde buffer
    const publicId = `wecars/${telefono.replace(/\D/g, "")}_${Date.now()}_${index}`;

    const resultado = await new Promise((resolve, reject) => {
      const uploadStream = cloudinary.uploader.upload_stream(
        {
          public_id:  publicId,
          folder:     "wecars-leads",
          resource_type: "image",
          format:     "webp",          // convierte a WebP (más ligero)
          quality:    "auto:good",     // optimización automática
          transformation: [{ width: 1200, crop: "limit" }], // max 1200px
        },
        (error, result) => {
          if (error) reject(error);
          else resolve(result);
        }
      );
      uploadStream.end(buffer);
    });

    console.log(`[Cloudinary] Foto ${index + 1} subida: ${resultado.secure_url}`);
    return resultado.secure_url;

  } catch (err) {
    console.error(`[Cloudinary] Error foto ${index}:`, err.message);
    return null; // No bloquea el flujo si falla una foto
  }
}

// ── OpenAI ────────────────────────────────────────────────────────────────────
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// ── PostgreSQL ────────────────────────────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS whatsapp_leads (
      id            SERIAL PRIMARY KEY,
      telefono      TEXT,
      nombre        TEXT,
      marca         TEXT,
      modelo        TEXT,
      version       TEXT,
      anio          INTEGER,
      kilometraje   INTEGER,
      precio        INTEGER,
      ciudad        TEXT,
      factura       TEXT,
      comentarios   TEXT,
      imagenes      TEXT[],
      prioridad     TEXT,
      faltantes     TEXT[],
      monday_item_id TEXT,
      publicado     BOOLEAN DEFAULT true,
      created_at    TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  console.log("[DB] Tabla whatsapp_leads lista");
}

// ── Buffer de mensajes por número (acumula antes de procesar) ─────────────────
// Estructura: { telefono: { mensajes: [], imagenes: [], nombre, timer } }
const buffer = new Map();
const BUFFER_TIMEOUT_MS = 30000; // 30 seg sin nuevos mensajes → procesa

function flushBuffer(telefono) {
  const entry = buffer.get(telefono);
  if (!entry) return;
  clearTimeout(entry.timer);
  buffer.delete(telefono);

  const { mensajes, imagenes, nombre } = entry;
  const textoTotal = mensajes.join("\n");
  const totalImagenes = imagenes.length;

  console.log(`[Buffer] Procesando ${telefono} | ${mensajes.length} msgs | ${totalImagenes} imgs`);
  procesarConversacion(textoTotal, telefono, nombre, totalImagenes, imagenes);
}

function agregarAlBuffer(telefono, nombre, mensaje, imgs) {
  if (!buffer.has(telefono)) {
    buffer.set(telefono, { mensajes: [], imagenes: [], nombre, timer: null });
  }
  const entry = buffer.get(telefono);
  entry.nombre = nombre;
  if (mensaje) entry.mensajes.push(mensaje);
  entry.imagenes.push(...imgs);

  // Reinicia el timer cada vez que llega un mensaje nuevo
  clearTimeout(entry.timer);
  entry.timer = setTimeout(() => flushBuffer(telefono), BUFFER_TIMEOUT_MS);

  console.log(`[Buffer] ${telefono} → ${entry.mensajes.length} msgs acumulados (esperando ${BUFFER_TIMEOUT_MS / 1000}s)`);
}

// ── Prompt estricto de OpenAI ─────────────────────────────────────────────────
const SYSTEM_PROMPT = `Eres un extractor de datos de vehículos usados en México para un marketplace automotriz.

REGLAS ESTRICTAS:
1. Devuelve ÚNICAMENTE un objeto JSON válido, sin texto adicional, sin markdown, sin bloques de código.
2. NUNCA inventes datos. Si no está explícito en el mensaje, el campo debe ser null.
3. precio y kilometraje deben ser números enteros (sin comas, sin puntos, sin "pesos", solo el número). Ejemplo: 185000, no "185,000" ni "$185,000".
4. anio debe ser un número entero de 4 dígitos. Ejemplo: 2019.
5. marca: detecta solo marcas reales de autos (Toyota, Honda, Nissan, Ford, Chevrolet, Kia, Hyundai, Audi, BMW, Mercedes-Benz, Volkswagen, Mazda, etc.). Si no está clara, usa null.
6. modelo: nombre del modelo específico del vehículo. Si no está claro, usa null.
7. intencion: usa "ofrecer_auto" solo si claramente están ofreciendo un vehículo para vender o consignar. Usa "otro" para cualquier otra cosa.
8. faltantes: lista de campos que faltan para tener el lead completo. Solo incluye: "marca", "modelo", "anio", "precio", "kilometraje", "ciudad", "fotos". Omite "fotos" si ya hay imágenes.
9. prioridad: "alta" si tiene marca + modelo + anio + precio + fotos. "media" si tiene al menos 3 de esos. "baja" en otro caso. null si no es ofrecer_auto.

FORMATO DE RESPUESTA (JSON puro, sin nada más):
{
  "intencion": "ofrecer_auto" | "otro",
  "marca": string | null,
  "modelo": string | null,
  "version": string | null,
  "anio": integer | null,
  "kilometraje": integer | null,
  "precio": integer | null,
  "ciudad": string | null,
  "factura": string | null,
  "comentarios": string | null,
  "faltantes": string[],
  "prioridad": "alta" | "media" | "baja" | null
}`;

async function analizarConIA(textoTotal, telefono, nombre, totalImagenes, imagenes) {
  const userContent = [
    `Teléfono: ${telefono}`,
    `Nombre: ${nombre || "desconocido"}`,
    `Imágenes adjuntas: ${totalImagenes}`,
    `Conversación completa:\n${textoTotal}`,
    totalImagenes > 0 ? `URLs de imágenes:\n${imagenes.map(i => i.url).join("\n")}` : "",
  ].filter(Boolean).join("\n\n");

  const completion = await openai.chat.completions.create({
    model: "gpt-4o",
    temperature: 0.1,
    messages: [
      { role: "system", content: SYSTEM_PROMPT },
      { role: "user", content: userContent },
    ],
    response_format: { type: "json_object" }, // fuerza JSON válido
  });

  const raw = completion.choices[0].message.content.trim();
  console.log("[IA raw]", raw);

  try {
    const parsed = JSON.parse(raw);

    // Sanitización extra: enteros o null
    const toInt = (v) => {
      if (v === null || v === undefined || v === "") return null;
      const n = parseInt(String(v).replace(/[^0-9]/g, ""), 10);
      return isNaN(n) ? null : n;
    };

    parsed.precio      = toInt(parsed.precio);
    parsed.kilometraje = toInt(parsed.kilometraje);
    parsed.anio        = toInt(parsed.anio);

    // Valida marca — no acepta frases genéricas
    const MARCAS_VALIDAS = ["toyota","honda","nissan","ford","chevrolet","gmc","kia","hyundai",
      "audi","bmw","mercedes","benz","volkswagen","vw","mazda","subaru","mitsubishi",
      "jeep","ram","dodge","chrysler","acura","infiniti","lexus","volvo","seat","peugeot",
      "renault","fiat","alfa","romeo","porsche","land","rover","jaguar","mini","lincoln",
      "buick","cadillac","tesla","suzuki","isuzu","haval","mg","geely","chery","jac"];
    if (parsed.marca) {
      const marcaLower = parsed.marca.toLowerCase();
      const valida = MARCAS_VALIDAS.some(m => marcaLower.includes(m));
      if (!valida) {
        console.log(`[IA] Marca inválida descartada: "${parsed.marca}"`);
        parsed.marca = null;
      }
    }

    return parsed;
  } catch (err) {
    console.error("[IA] Error parseando JSON:", err.message, "| Raw:", raw);
    return { intencion: "error_parseo", faltantes: [], prioridad: null };
  }
}

// ── Procesa la conversación completa del buffer ───────────────────────────────
async function procesarConversacion(textoTotal, telefono, nombre, totalImagenes, imagenes) {
  try {
    const clasificacion = await analizarConIA(textoTotal, telefono, nombre, totalImagenes, imagenes);
    console.log("[IA]", JSON.stringify(clasificacion));

    if (clasificacion.intencion !== "ofrecer_auto") {
      console.log(`[Bot] ${telefono} no es ofrecer_auto (${clasificacion.intencion}). No se crea item.`);
      return;
    }

    // Requiere al menos marca o modelo para crear item
    if (!clasificacion.marca && !clasificacion.modelo) {
      console.log(`[Bot] ${telefono} sin marca ni modelo. No se crea item.`);
      return;
    }

    const telefonoLimpio = telefono.replace("whatsapp:", "");
    const comentariosCompletos = [
      `Nombre: ${nombre}`,
      `Tel: ${telefonoLimpio}`,
      `Prioridad: ${clasificacion.prioridad || "—"}`,
      clasificacion.comentarios || "",
      `Faltantes: ${(clasificacion.faltantes || []).join(", ") || "ninguno"}`,
      totalImagenes > 0 ? `Imágenes (${totalImagenes}): ${imagenes.map(i => i.url).join(" | ")}` : "",
    ].filter(Boolean).join("\n");

    const urlsFotos = imagenes.map(i => i.url).filter(Boolean);

    // 1️⃣ Monday.com — un solo item por conversación
    const mondayId = await crearItemMonday({
      marca:        clasificacion.marca,
      modelo:       clasificacion.modelo,
      version:      clasificacion.version,
      anio:         clasificacion.anio,
      kilometraje:  clasificacion.kilometraje,
      precio:       clasificacion.precio,
      ciudad:       clasificacion.ciudad,
      factura:      clasificacion.factura,
      telefono:     telefonoLimpio,
      comentarios:  comentariosCompletos,
      urlsFotos,
    });
    console.log("[Monday] Item creado:", mondayId);

    // 2️⃣ PostgreSQL
    const dbId = await guardarEnDB({
      telefono:    telefonoLimpio,
      nombre,
      marca:       clasificacion.marca,
      modelo:      clasificacion.modelo,
      version:     clasificacion.version,
      anio:        clasificacion.anio,
      kilometraje: clasificacion.kilometraje,
      precio:      clasificacion.precio,
      ciudad:      clasificacion.ciudad,
      factura:     clasificacion.factura,
      comentarios: comentariosCompletos,
      imagenes:    imagenes.map(i => i.url),
      prioridad:   clasificacion.prioridad,
      faltantes:   clasificacion.faltantes,
    }, mondayId);
    console.log("[DB] Lead guardado id:", dbId, "→ weMarket ✅");

  } catch (err) {
    console.error("[Error procesarConversacion]:", err.response?.data || err.message);
  }
}

// ── Monday.com ────────────────────────────────────────────────────────────────
async function crearItemMonday(datos) {
  const columnValues = {
    text_mm3hz3ps:    datos.marca    || "",
    text_mm3hnpfp:    datos.modelo   || "",
    text_mm3h4yrh:    datos.version  || "",
    numeric_mm3h6v7:  datos.anio         ?? null,
    numeric_mm3h45jr: datos.kilometraje  ?? null,
    numeric_mm3ha16x: datos.precio       ?? null,
    text_mm3hx5k:     datos.ciudad   || "",
    text_mm3hdqs4:    datos.factura  || "",
    phone_mm3hh4n:    { phone: datos.telefono || "", countryShortName: "MX" },
    long_text_mm3hvzwc: [
      datos.comentarios || "",
      datos.urlsFotos?.length
        ? `\n📷 FOTOS (${datos.urlsFotos.length}):\n${datos.urlsFotos.map((u, i) => `Foto ${i + 1}: ${u}`).join("\n")}`
        : "",
    ].filter(Boolean).join("\n"),
    color_mm3htx5t:   { label: "Pendiente" },
  };

  const query = `
    mutation ($boardId: ID!, $itemName: String!, $columnValues: JSON!) {
      create_item(board_id: $boardId, item_name: $itemName, column_values: $columnValues) { id }
    }
  `;

  const itemName = [datos.marca, datos.modelo, datos.anio].filter(Boolean).join(" ") || "AUTO sin datos";

  const response = await axios.post(
    "https://api.monday.com/v2",
    { query, variables: { boardId: process.env.MONDAY_BOARD_ID, itemName, columnValues: JSON.stringify(columnValues) } },
    { headers: { Authorization: process.env.MONDAY_API_KEY, "Content-Type": "application/json" } }
  );

  return response.data?.data?.create_item?.id || null;
}

// ── PostgreSQL insert ─────────────────────────────────────────────────────────
async function guardarEnDB(datos, mondayId) {
  const result = await pool.query(
    `INSERT INTO whatsapp_leads
       (telefono, nombre, marca, modelo, version, anio, kilometraje, precio,
        ciudad, factura, comentarios, imagenes, prioridad, faltantes, monday_item_id, publicado)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
     RETURNING id`,
    [
      datos.telefono, datos.nombre, datos.marca, datos.modelo, datos.version,
      datos.anio ?? null, datos.kilometraje ?? null, datos.precio ?? null,
      datos.ciudad, datos.factura, datos.comentarios,
      datos.imagenes || [], datos.prioridad, datos.faltantes || [], mondayId,
      false, // publicado = false por defecto → se activa desde Monday al aprobar
    ]
  );
  return result.rows[0].id;
}

// ── GET / ─────────────────────────────────────────────────────────────────────
app.get("/", (req, res) => {
  res.json({
    status: "ok",
    service: "WeCars WhatsApp Bot",
    buffer: buffer.size,
    message: "WhatsApp + OpenAI + PostgreSQL + Monday ✅",
  });
});

// ── GET /vehiculos — Lee del tablero weCars Market de Monday (publicados) ─────
const MONDAY_MARKET_BOARD_ID = process.env.MONDAY_MARKET_BOARD_ID || "18414157675";

async function obtenerVehiculosMonday() {
  const query = `
    query ($boardId: ID!) {
      boards(ids: [$boardId]) {
        items_page(limit: 50, query_params: { order_by: [{ column_id: "creation_log__1", direction: desc }] }) {
          items {
            id
            name
            created_at
            column_values {
              id
              text
              value
            }
          }
        }
      }
    }
  `;

  const response = await axios.post(
    "https://api.monday.com/v2",
    { query, variables: { boardId: MONDAY_MARKET_BOARD_ID } },
    { headers: { Authorization: process.env.MONDAY_API_KEY, "Content-Type": "application/json" } }
  );

  const items = response.data?.data?.boards?.[0]?.items_page?.items || [];

  return items.map(item => {
    // Convierte el array de column_values a un objeto { colId: text }
    const cols = {};
    for (const cv of item.column_values) {
      cols[cv.id] = cv.text || "";
    }

    // Extrae URLs de Cloudinary del campo de comentarios/long_text
    const longText = item.column_values.find(c => c.id === "long_text_mm3hvzwc")?.text || "";
    const fotoRegex = /https:\/\/res\.cloudinary\.com\/[^\s]+/g;
    const imagenes = longText.match(fotoRegex) || [];

    // Teléfono viene como objeto JSON en el value
    let telefono = "";
    try {
      const phoneVal = item.column_values.find(c => c.id === "phone_mm3hh4n");
      if (phoneVal?.value) {
        const parsed = JSON.parse(phoneVal.value);
        telefono = parsed.phone || "";
      }
    } catch (_) {}

    return {
      id:          item.id,
      monday_id:   item.id,
      nombre:      item.name,
      marca:       cols["text_mm3hz3ps"]    || null,
      modelo:      cols["text_mm3hnpfp"]    || null,
      version:     cols["text_mm3h4yrh"]    || null,
      anio:        parseInt(cols["numeric_mm3h6v7"])  || null,
      kilometraje: parseInt(cols["numeric_mm3h45jr"]) || null,
      precio:      parseInt(cols["numeric_mm3ha16x"]) || null,
      ciudad:      cols["text_mm3hx5k"]     || null,
      factura:     cols["text_mm3hdqs4"]    || null,
      telefono,
      imagenes,
      created_at:  item.created_at,
    };
  });
}

app.get("/vehiculos", async (req, res) => {
  try {
    const vehiculos = await obtenerVehiculosMonday();
    res.json({ ok: true, total: vehiculos.length, vehiculos });
  } catch (err) {
    console.error("[Monday Market] Error:", err.message);
    // Fallback a PostgreSQL si Monday falla
    try {
      const result = await pool.query(
        `SELECT id, marca, modelo, version, anio, kilometraje, precio,
                ciudad, factura, imagenes, prioridad, telefono, nombre, created_at
         FROM whatsapp_leads WHERE publicado = true ORDER BY created_at DESC LIMIT 50`
      );
      res.json({ ok: true, total: result.rows.length, vehiculos: result.rows, source: "db_fallback" });
    } catch (dbErr) {
      res.status(500).json({ ok: false, error: err.message });
    }
  }
});

// ── POST /webhook — Twilio WhatsApp ───────────────────────────────────────────
app.post("/webhook", async (req, res) => {
  // Responde de inmediato a Twilio (evita timeout de 15s)
  res.status(200).send("ok");

  try {
    const mensaje = req.body.Body || "";
    const telefono = req.body.From || "";
    const nombre = req.body.ProfileName || "";
    const totalImagenes = Number(req.body.NumMedia || 0);
    const imagenes = [];

    // Sube fotos a Cloudinary en paralelo (URLs públicas permanentes)
    const subidas = await Promise.all(
      Array.from({ length: totalImagenes }, (_, i) =>
        subirFotoCloudinary(req.body[`MediaUrl${i}`], i, telefono)
      )
    );
    for (let i = 0; i < totalImagenes; i++) {
      const urlPublica = subidas[i];
      imagenes.push({
        url:  urlPublica || req.body[`MediaUrl${i}`], // fallback a Twilio si falla Cloudinary
        tipo: req.body[`MediaContentType${i}`],
        cloudinary: !!urlPublica,
      });
    }

    console.log(`[WhatsApp] ${nombre || telefono} | msg: "${mensaje.slice(0, 60)}" | imgs: ${totalImagenes} (${subidas.filter(Boolean).length} en Cloudinary)`);

    // Acumula en buffer — procesa después de 30s de silencio
    agregarAlBuffer(telefono, nombre, mensaje, imagenes);

  } catch (err) {
    console.error("[Error webhook]:", err.message);
  }
});

// ── POST /monday-publish — Monday llama esto cuando se hace click en "Publicar" ──
// Monday envía un challenge la primera vez para verificar el endpoint
app.post("/monday-publish", async (req, res) => {
  // Verificación de Monday (handshake inicial)
  if (req.body.challenge) {
    console.log("[Monday Webhook] Challenge recibido, respondiendo...");
    return res.json({ challenge: req.body.challenge });
  }

  const itemId = (req.body.event?.pulseId || req.body.event?.itemId)?.toString();
  if (!itemId) {
    console.log("[Monday Webhook] Sin itemId en el body:", JSON.stringify(req.body));
    return res.status(400).json({ error: "No itemId" });
  }

  console.log(`[Monday Webhook] Publicando item Monday: ${itemId}`);

  try {
    const result = await pool.query(
      `UPDATE whatsapp_leads SET publicado = true WHERE monday_item_id = $1 RETURNING id, marca, modelo`,
      [itemId]
    );

    if (result.rows.length === 0) {
      console.log(`[Monday Webhook] Item ${itemId} no encontrado en DB`);
      return res.json({ ok: false, msg: "No encontrado en DB" });
    }

    const { id, marca, modelo } = result.rows[0];
    console.log(`[Monday Webhook] ✅ Publicado en weMarket: ${marca} ${modelo} (DB id: ${id})`);
    res.json({ ok: true, id });

  } catch (err) {
    console.error("[Monday Webhook] Error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Arranque ──────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;

initDB().then(() => {
  app.listen(PORT, () => console.log(`[WeCars Bot] Puerto ${PORT} ✅`));
}).catch((err) => {
  console.error("[DB] Error al inicializar:", err.message);
  process.exit(1);
});
