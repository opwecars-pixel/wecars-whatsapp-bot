const express = require("express");
const OpenAI = require("openai");
const axios = require("axios");
const { Pool } = require("pg");

const app = express();

app.use(express.urlencoded({ extended: true }));
app.use(express.json());

// ── CORS para weMarket Zone ────────────────────────────────────────────────────
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Content-Type");
  res.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

// ── OpenAI ────────────────────────────────────────────────────────────────────
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

// ── PostgreSQL ────────────────────────────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS whatsapp_leads (
      id SERIAL PRIMARY KEY,
      telefono TEXT,
      nombre TEXT,
      marca TEXT,
      modelo TEXT,
      version TEXT,
      anio INTEGER,
      kilometraje INTEGER,
      precio NUMERIC,
      ciudad TEXT,
      factura TEXT,
      comentarios TEXT,
      imagenes TEXT[],
      prioridad TEXT,
      faltantes TEXT[],
      monday_item_id TEXT,
      publicado BOOLEAN DEFAULT true,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  console.log("[DB] Tabla whatsapp_leads lista");
}

// ── Monday.com ────────────────────────────────────────────────────────────────
async function crearItemMonday(datos) {
  const columnValues = {
    text_mm3hz3ps: datos.marca || "",
    text_mm3hnpfp: datos.modelo || "",
    text_mm3h4yrh: datos.version || "",
    numeric_mm3h6v7: datos.anio ? Number(datos.anio) : null,
    numeric_mm3h45jr: datos.kilometraje ? Number(datos.kilometraje) : null,
    numeric_mm3ha16x: datos.precio ? Number(datos.precio) : null,
    text_mm3hx5k: datos.ciudad || "",
    text_mm3hdqs4: datos.factura || "",
    phone_mm3hh4n: {
      phone: datos.telefono || "",
      countryShortName: "MX",
    },
    long_text_mm3hvzwc: datos.comentarios || "",
    color_mm3htx5t: { label: "Pendiente" },
  };

  const query = `
    mutation ($boardId: ID!, $itemName: String!, $columnValues: JSON!) {
      create_item (
        board_id: $boardId,
        item_name: $itemName,
        column_values: $columnValues
      ) { id }
    }
  `;

  const variables = {
    boardId: process.env.MONDAY_BOARD_ID,
    itemName: `${datos.marca || "AUTO"} ${datos.modelo || ""} ${datos.anio || ""}`.trim(),
    columnValues: JSON.stringify(columnValues),
  };

  const response = await axios.post(
    "https://api.monday.com/v2",
    { query, variables },
    {
      headers: {
        Authorization: process.env.MONDAY_API_KEY,
        "Content-Type": "application/json",
      },
    }
  );

  return response.data?.data?.create_item?.id || null;
}

// ── Guardar en PostgreSQL ─────────────────────────────────────────────────────
async function guardarEnDB(datos, mondayId) {
  const result = await pool.query(
    `INSERT INTO whatsapp_leads
      (telefono, nombre, marca, modelo, version, anio, kilometraje, precio,
       ciudad, factura, comentarios, imagenes, prioridad, faltantes, monday_item_id)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
     RETURNING id`,
    [
      datos.telefono,
      datos.nombre,
      datos.marca,
      datos.modelo,
      datos.version,
      datos.anio ? Number(datos.anio) : null,
      datos.kilometraje ? Number(datos.kilometraje) : null,
      datos.precio ? Number(datos.precio) : null,
      datos.ciudad,
      datos.factura,
      datos.comentarios,
      datos.imagenes || [],
      datos.prioridad,
      datos.faltantes || [],
      mondayId,
    ]
  );
  return result.rows[0].id;
}

// ── Prompt de OpenAI ──────────────────────────────────────────────────────────
const SYSTEM_PROMPT = `
Extrae información de vehículos usados en México.
Devuelve SOLO JSON válido.

Formato:
{
  "intencion": "",
  "marca": "",
  "modelo": "",
  "version": "",
  "anio": "",
  "kilometraje": "",
  "precio": "",
  "ciudad": "",
  "factura": "",
  "comentarios": "",
  "faltantes": [],
  "prioridad": ""
}

Reglas:
- Si no detectas algún dato, déjalo vacío.
- Si parece que quieren vender/ofrecer un auto, usa intencion: "ofrecer_auto".
- Si el mensaje no trata de un auto, usa intencion: "otro".
- Si viene al menos una imagen, no pongas "fotos" como faltante.
- Prioridad alta si trae marca, modelo, año, precio y fotos.
`;

async function analizarConIA(mensaje, telefono, nombre, totalImagenes, imagenes) {
  const completion = await openai.chat.completions.create({
    model: "gpt-4.1-mini",
    messages: [
      { role: "system", content: SYSTEM_PROMPT },
      {
        role: "user",
        content: `
Mensaje: ${mensaje}
Teléfono: ${telefono}
Nombre: ${nombre}
Cantidad de imágenes: ${totalImagenes}
Imágenes: ${imagenes.map((img) => img.url).join("\n")}
`,
      },
    ],
  });

  let respuesta = completion.choices[0].message.content
    .replace(/```json/g, "")
    .replace(/```/g, "")
    .trim();

  try {
    return JSON.parse(respuesta);
  } catch {
    return { intencion: "error_parseo", comentarios: respuesta };
  }
}

// ── GET / ─────────────────────────────────────────────────────────────────────
app.get("/", (req, res) => {
  res.send("WeCars Bot: WhatsApp + OpenAI + PostgreSQL + Monday ✅");
});

// ── GET /vehiculos — API pública para weMarket Zone ──────────────────────────
app.get("/vehiculos", async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT id, marca, modelo, version, anio, kilometraje, precio,
              ciudad, factura, imagenes, prioridad, created_at
       FROM whatsapp_leads
       WHERE publicado = true
       ORDER BY created_at DESC
       LIMIT 50`
    );
    res.json({ ok: true, total: result.rows.length, vehiculos: result.rows });
  } catch (err) {
    console.error("[DB] Error al obtener vehículos:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ── POST /webhook — Twilio WhatsApp ───────────────────────────────────────────
app.post("/webhook", async (req, res) => {
  try {
    const mensaje = req.body.Body || "";
    const telefono = req.body.From || "";
    const nombre = req.body.ProfileName || "";
    const totalImagenes = Number(req.body.NumMedia || 0);
    const imagenes = [];

    for (let i = 0; i < totalImagenes; i++) {
      imagenes.push({
        url: req.body[`MediaUrl${i}`],
        tipo: req.body[`MediaContentType${i}`],
      });
    }

    console.log(`[WhatsApp] ${nombre} (${telefono}) | imgs: ${totalImagenes}`);

    const clasificacion = await analizarConIA(mensaje, telefono, nombre, totalImagenes, imagenes);
    console.log("[IA]", clasificacion);

    if (clasificacion.intencion === "ofrecer_auto") {
      const telefonoLimpio = telefono.replace("whatsapp:", "");
      const comentariosCompletos = `Nombre: ${nombre}\nMensaje: ${mensaje}\n${clasificacion.comentarios || ""}\nFaltantes: ${(clasificacion.faltantes || []).join(", ")}\nPrioridad: ${clasificacion.prioridad || ""}\nImágenes: ${imagenes.map((i) => i.url).join(" | ")}`;

      // 1️⃣ Monday.com
      const mondayId = await crearItemMonday({
        marca: clasificacion.marca,
        modelo: clasificacion.modelo,
        version: clasificacion.version,
        anio: clasificacion.anio,
        kilometraje: clasificacion.kilometraje,
        precio: clasificacion.precio,
        ciudad: clasificacion.ciudad,
        factura: clasificacion.factura,
        telefono: telefonoLimpio,
        comentarios: comentariosCompletos,
      });
      console.log("[Monday] Item:", mondayId);

      // 2️⃣ PostgreSQL → se publica en weMarket automáticamente
      const dbId = await guardarEnDB({
        telefono: telefonoLimpio,
        nombre,
        marca: clasificacion.marca,
        modelo: clasificacion.modelo,
        version: clasificacion.version,
        anio: clasificacion.anio,
        kilometraje: clasificacion.kilometraje,
        precio: clasificacion.precio,
        ciudad: clasificacion.ciudad,
        factura: clasificacion.factura,
        comentarios: comentariosCompletos,
        imagenes: imagenes.map((i) => i.url),
        prioridad: clasificacion.prioridad,
        faltantes: clasificacion.faltantes,
      }, mondayId);
      console.log("[DB] Lead id:", dbId, "→ publicado en weMarket ✅");
    }

    res.status(200).send("ok");
  } catch (error) {
    console.error("[Error]:", error.response?.data || error.message);
    res.status(500).send("error");
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
