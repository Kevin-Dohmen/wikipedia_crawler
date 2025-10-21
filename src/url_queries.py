import psycopg2 as pg
from psycopg2.extras import execute_values

class urlModel:
    def __init__(self, id: int, url: str, status: bool | None):
        self.id = id
        self.url = url
        self.status = status

def check_url_exists(url: str, cur) -> bool:
    cur.execute("SELECT 1 FROM found_urls WHERE url = %s", (url,))
    return cur.fetchone() is not None

def add_url(url: str, cur, con) -> urlModel | None:
    """
    Insert a single URL and return the created/found urlModel.
    Uses ON CONFLICT DO NOTHING (no column target) to avoid requiring a unique index on url.
    """
    try:
        cur.execute(
            "INSERT INTO found_urls (url) VALUES (%s) ON CONFLICT DO NOTHING RETURNING id, url, status",
            (url,)
        )
        result = cur.fetchone()
        if result:
            con.commit()
            return urlModel(*result)

        # Insert didn't return a row because it existed already or lost to race -> select the existing row
        cur.execute("SELECT id, url, status FROM found_urls WHERE url = %s", (url,))
        row = cur.fetchone()
        con.commit()
        return urlModel(*row) if row else None
    except pg.Error:
        try:
            con.rollback()
        except Exception:
            pass
        # On error try to read existing row (best-effort)
        try:
            cur.execute("SELECT id, url, status FROM found_urls WHERE url = %s", (url,))
            row = cur.fetchone()
            con.commit()
            return urlModel(*row) if row else None
        except Exception:
            try:
                con.rollback()
            except Exception:
                pass
            return None

def add_urls(urls: list[str], cur, con) -> list[urlModel]:
    if not urls:
        return []
    # keep input order but dedupe
    unique_urls = list(dict.fromkeys(urls))

    sql = """
        SELECT
            id,
            url,
            status
        FROM
            found_urls
        WHERE
            url = ANY ( %s );
    """
    cur.execute(sql, (unique_urls,))
    existing_rows = {row[1]: urlModel(*row) for row in cur.fetchall()}
    new_urls = [url for url in unique_urls if url not in existing_rows]

    inserted_urls = []
    if new_urls:
        insert_sql = "INSERT INTO found_urls (url) VALUES %s RETURNING id, url, status"
        values = [(url,) for url in new_urls]
        execute_values(cur, insert_sql, values, fetch=True)

        for row in cur.fetchall():
            inserted_urls.append(urlModel(*row))

    con.commit()

    inserted_map = {u.url: u for u in inserted_urls}
    result: list[urlModel] = []
    for url in unique_urls:
        if url in existing_rows:
            result.append(existing_rows[url])
        elif url in inserted_map:
            result.append(inserted_map[url])

    return result

def add_urls_nocommit(urls: list[str], cur) -> list[urlModel]:
    if not urls:
        return []
    # keep input order but dedupe
    unique_urls = list(dict.fromkeys(urls))

    sql = """
        SELECT
            id,
            url,
            status
        FROM
            found_urls
        WHERE
            url = ANY ( %s );
    """
    cur.execute(sql, (unique_urls,))
    existing_rows = {row[1]: urlModel(*row) for row in cur.fetchall()}
    new_urls = [url for url in unique_urls if url not in existing_rows]

    inserted_urls = []
    if new_urls:
        insert_sql = "INSERT INTO found_urls (url) VALUES %s RETURNING id, url, status"
        values = [(url,) for url in new_urls]
        execute_values(cur, insert_sql, values, fetch=True)

        for row in cur.fetchall():
            inserted_urls.append(urlModel(*row)) 

    inserted_map = {u.url: u for u in inserted_urls}
    result: list[urlModel] = []
    for url in unique_urls:
        if url in existing_rows:
            result.append(existing_rows[url])
        elif url in inserted_map:
            result.append(inserted_map[url])

    return result

def url_set_error(id: int, cur, con):
    cur.execute("UPDATE found_urls SET error = TRUE WHERE id = %s", (id,))
    con.commit()

def url_set_error_nocommit(id: int, cur):
    cur.execute("UPDATE found_urls SET error = TRUE WHERE id = %s", (id,))

def url_clear_error(id: int, cur, con):
    cur.execute("UPDATE found_urls SET error = FALSE WHERE id = %s", (id,))
    con.commit()

def url_clear_error_nocommit(id: int, cur):
    cur.execute("UPDATE found_urls SET error = FALSE WHERE id = %s", (id,))

def delete_url(id: int, cur, con):
    cur.execute("DELETE FROM found_urls WHERE id = %s", (id,))
    con.commit()

def get_unscanned_urls(cur) -> list[urlModel]:
    cur.execute("""
        SELECT
            u.id AS id,
            u.url AS url,
            u.status AS status
        FROM
            found_urls u
        WHERE
            status IS NULL AND
            error = FALSE
        LIMIT 100;
    """)
    result = cur.fetchall()
    return [urlModel(*row) for row in result] if result else []

def mark_url_as_scanning(id: int, cur, con):
    cur.execute("UPDATE found_urls SET status = FALSE WHERE id = %s", (id,))
    con.commit()

def mark_url_as_scanned(id: int, cur, con):
    cur.execute("UPDATE found_urls SET status = TRUE WHERE id = %s", (id,))
    con.commit()

def mark_url_as_scanned_nocommit(id: int, cur):
    cur.execute("UPDATE found_urls SET status = TRUE WHERE id = %s", (id,))

def mark_url_as_unscanned(id: int, cur, con):
    cur.execute("UPDATE found_urls SET status = NULL WHERE id = %s", (id,))
    con.commit()

def mark_url_as_unscanned_nocommit(id: int, cur):
    cur.execute("UPDATE found_urls SET status = NULL WHERE id = %s", (id,))

def add_url_relation(referencing_url_id: int, referenced_url_id: int, cur, con):
    cur.execute("""
        INSERT INTO url_relations (referencing_url, referenced_url)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
    """, (referencing_url_id, referenced_url_id))
    con.commit()

def add_url_relation_nocommit(referencing_url_id: int, referenced_url_id: int, cur):
    cur.execute("""
        INSERT INTO url_relations (referencing_url, referenced_url)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
    """, (referencing_url_id, referenced_url_id))

def add_url_relations(referencing_url_id: int, referenced_url_ids: list[int], cur, con):
    if not referenced_url_ids:
        return
    values = [(referencing_url_id, ref_id) for ref_id in referenced_url_ids]
    sql = """
        INSERT INTO url_relations (referencing_url, referenced_url)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    execute_values(cur, sql, values)
    con.commit()

def add_url_relations_nocommit(referencing_url_id: int, referenced_url_ids: list[int], cur):
    if not referenced_url_ids:
        return
    values = [(referencing_url_id, ref_id) for ref_id in referenced_url_ids]
    sql = """
        INSERT INTO url_relations (referencing_url, referenced_url)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    execute_values(cur, sql, values)
