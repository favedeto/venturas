import os
import asyncio
import xmlrpc.client
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, Header
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import asyncpg

load_dotenv()

ODOO_URL = os.environ["ODOO_URL"]
ODOO_DB = os.environ["ODOO_DB"]
ODOO_USER = os.environ["ODOO_USER"]
ODOO_PASSWORD = os.environ["ODOO_PASSWORD"]
DATABASE_URL = os.environ["DATABASE_URL"]
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")

db_pool: asyncpg.Pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS productos (
                id          SERIAL PRIMARY KEY,
                odoo_id     INTEGER UNIQUE NOT NULL,
                nombre      TEXT NOT NULL,
                codigo      TEXT,
                precio_lista NUMERIC(12,2) NOT NULL,
                actualizado_en TIMESTAMPTZ DEFAULT NOW()
            )
        """)
    yield
    await db_pool.close()


app = FastAPI(lifespan=lifespan)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _odoo_fetch_all() -> list[dict]:
    """Conecta a Odoo via XML-RPC y devuelve todos los productos activos."""
    common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
    uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_PASSWORD, {})
    if not uid:
        raise ValueError("Credenciales de Odoo inválidas")
    models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")
    return models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        "product.template", "search_read",
        [[["active", "=", True]]],
        {"fields": ["id", "name", "list_price", "default_code"]},
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/productos")
async def get_productos():
    """
    Devuelve el catálogo de productos en formato JSON desde PostgreSQL.
    Ejecute POST /sync primero para poblar la base de datos.
    """
    rows = await db_pool.fetch(
        "SELECT odoo_id AS id, nombre, codigo, precio_lista::float, "
        "actualizado_en::text FROM productos ORDER BY nombre"
    )
    if not rows:
        raise HTTPException(
            status_code=503,
            detail="La base de datos de productos está vacía. Ejecute POST /sync primero.",
        )
    return JSONResponse(content={"productos": [dict(r) for r in rows]})


@app.post("/sync")
async def sync_productos():
    """
    Sincronización completa: lee todos los productos activos de Odoo
    y los inserta/actualiza en PostgreSQL (upsert por odoo_id).
    Ejecutar manualmente o programar como tarea periódica.
    """
    try:
        productos = await asyncio.to_thread(_odoo_fetch_all)
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Error al conectar con Odoo: {e}")

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            for p in productos:
                await conn.execute(
                    """
                    INSERT INTO productos (odoo_id, nombre, codigo, precio_lista, actualizado_en)
                    VALUES ($1, $2, $3, $4, NOW())
                    ON CONFLICT (odoo_id) DO UPDATE
                        SET nombre        = EXCLUDED.nombre,
                            codigo        = EXCLUDED.codigo,
                            precio_lista  = EXCLUDED.precio_lista,
                            actualizado_en = NOW()
                    """,
                    int(p["id"]),
                    str(p.get("name", "")),
                    p.get("default_code") or None,
                    float(p.get("list_price", 0)),
                )

    return {"sincronizados": len(productos)}


@app.post("/webhook/odoo")
async def webhook_odoo(
    request: Request,
    x_odoo_secret: str = Header(default=""),
):
    """
    Webhook para actualizaciones en tiempo real desde Odoo.

    Configuración en Odoo:
      Ajustes > Técnico > Automatización > Acciones Automáticas
      Modelo: product.template | Disparador: Al guardar (precio cambia)
      Acción: Ejecutar código Python →
        import requests
        requests.post(
            'https://<tu-servidor>/webhook/odoo',
            json={
                'id': record.id,
                'name': record.name,
                'list_price': record.list_price,
                'default_code': record.default_code,
            },
            headers={'X-Odoo-Secret': '<WEBHOOK_SECRET>'},
            timeout=5,
        )

    Encabezado requerido: X-Odoo-Secret: <valor de WEBHOOK_SECRET en .env>
    Payload JSON: { "id": int, "name": str, "list_price": float, "default_code": str|null }
    """
    if WEBHOOK_SECRET and x_odoo_secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Token de webhook inválido")

    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Payload JSON inválido")

    odoo_id = data.get("id")
    nombre = data.get("name")
    precio = data.get("list_price")

    if odoo_id is None or nombre is None or precio is None:
        raise HTTPException(
            status_code=422,
            detail="Campos requeridos en el payload: id, name, list_price",
        )

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO productos (odoo_id, nombre, codigo, precio_lista, actualizado_en)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (odoo_id) DO UPDATE
                SET nombre        = EXCLUDED.nombre,
                    codigo        = EXCLUDED.codigo,
                    precio_lista  = EXCLUDED.precio_lista,
                    actualizado_en = NOW()
            """,
            int(odoo_id),
            str(nombre),
            data.get("default_code") or None,
            float(precio),
        )

    return {"actualizado": odoo_id, "nuevo_precio": precio}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)