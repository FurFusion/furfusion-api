// worker/src/index.ts
// =====================================================
// FurFusion API Worker (D1 + Stripe + Resend + Blogs)
// PART 1/3 — includes: Health, Public Reviews (+summary), Orders (public),
// Checkout + Webhook (stub entrypoint), Public Blogs + Admin Blogs
// =====================================================

console.log("PATH:", path);

export interface Env {
  DB: D1Database;

  // Admin
  ADMIN_EMAIL: string; // "a@b.com" or "a@b.com,b@c.com"

  // Stripe
  STRIPE_SECRET_KEY: string;
  STRIPE_WEBHOOK_SECRET: string;
  STRIPE_PRICE_ID: string;
  SITE_URL: string;

  // Resend
  RESEND_API_KEY?: string;
  FROM_EMAIL?: string;
  LOGO_URL?: string;

  // Review request email security
  REVIEW_LINK_SECRET?: string; // random long secret string
}

// =====================================================
// BLOG TYPES / PARSER (declare ONCE)
// content in DB is JSON string: { excerpt?, coverImage?, coverAlt?, blocks:[...] }
// =====================================================
type BlogContent = {
  excerpt?: string;
  coverImage?: string;
  coverAlt?: string;
  blocks: Array<
    | { type: "h2"; text: string }
    | { type: "p"; text: string }
    | { type: "ul"; items: string[] }
    | { type: "img"; url: string; alt?: string; caption?: string } // admin UI
    | { type: "image"; url: string; alt: string; caption?: string } // backward compatibility
  >;
};

function safeJson<T>(s: any, fallback: T): T {
  try {
    const v = JSON.parse(String(s ?? ""));
    return (v ?? fallback) as T;
  } catch {
    return fallback;
  }
}

function normalizeBlogContent(input: any): BlogContent {
  const obj = typeof input === "string" ? safeJson<any>(input, null) : input;

  const fallback: BlogContent = { blocks: [] };
  if (!obj || typeof obj !== "object") return fallback;

  const excerpt = typeof obj.excerpt === "string" ? obj.excerpt.trim() : undefined;
  const coverImage = typeof obj.coverImage === "string" ? obj.coverImage.trim() : undefined;
  const coverAlt = typeof obj.coverAlt === "string" ? obj.coverAlt.trim() : undefined;

  const blocksIn = Array.isArray(obj.blocks) ? obj.blocks : [];
  const blocks: BlogContent["blocks"] = [];

  for (const b of blocksIn) {
    if (!b || typeof b !== "object") continue;
    const t = String((b as any).type || "").trim();

    if (t === "h2") {
      const text = String((b as any).text || "").trim();
      if (text) blocks.push({ type: "h2", text });
      continue;
    }

    if (t === "p") {
      const text = String((b as any).text || "").trim();
      blocks.push({ type: "p", text });
      continue;
    }

    if (t === "ul") {
      const items = Array.isArray((b as any).items)
        ? (b as any).items.map((x: any) => String(x).trim()).filter(Boolean)
        : [];
      blocks.push({ type: "ul", items });
      continue;
    }

    if (t === "img" || t === "image") {
      const url = String((b as any).url || "").trim();
      if (!url) continue;

      const alt = String((b as any).alt || "").trim();
      const caption =
        typeof (b as any).caption === "string" ? String((b as any).caption).trim() : undefined;

      if (t === "img") blocks.push({ type: "img", url, alt: alt || undefined, caption });
      else blocks.push({ type: "image", url, alt: alt || "", caption });

      continue;
    }
  }

  return { excerpt, coverImage, coverAlt, blocks };
}

// =====================================================
// Worker
// =====================================================
export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const headers = cors(req);

    // Preflight
    if (req.method === "OPTIONS") {
      return new Response(null, { status: 204, headers });
    }

    try {
      if (!env.DB) return json({ ok: false, error: "DB binding missing" }, headers, 500);

      const url = new URL(req.url);
      const path = url.pathname;

      // ---------------------
      // Health
      // ---------------------
      if (req.method === "GET" && path === "/") {
        return json({ ok: true, service: "furfusion-api" }, headers);
      }

      // ---------------------
      // Admin debug: whoami
      // ---------------------
      if (req.method === "GET" && path === "/api/admin/whoami") {
        const email =
          req.headers.get("Cf-Access-Authenticated-User-Email") ||
          req.headers.get("cf-access-authenticated-user-email") ||
          "";

        return json(
          {
            ok: true,
            email,
            cookie: req.headers.get("Cookie") || "",
            hasAccessCookie: (req.headers.get("Cookie") || "").includes("CF_Authorization="),
          },
          headers
        );
      }
      

// =====================================================
// Admin: Analytics – Sales & Orders
// GET /api/admin/analytics/sales?range=7d
// Cloudflare Access protected
// =====================================================
if (req.method === "GET" && path.endsWith("/analytics/sales")) {
  const email = getAccessEmail(req);
  if (!email || !isAdmin(req, env)) return json({ ok: false, error: "Unauthorized" }, headers, 401);

  const range = url.searchParams.get("range") || "7d";
  let daysLimit: number | null = null;
  
  // Mapping ranges to days
  if (range === "today") daysLimit = 1;
  else if (range === "7d") daysLimit = 7;
  else if (range === "30d") daysLimit = 30;
  else if (range === "all") daysLimit = null;

  // We use DATE() instead of DATETIME() for better compatibility with string dates
  const dateFilter = daysLimit 
    ? `AND date(created_at) >= date('now', '-${daysLimit} days')` 
    : "";

  const { results } = await env.DB.prepare(`
    SELECT 
      DATE(created_at) as day, 
      COUNT(*) as orders, 
      SUM(total) as revenue
    FROM orders 
    WHERE payment_status = 'paid'
    ${dateFilter}
    GROUP BY day 
    ORDER BY day ASC
  `).all();

  return json({
    ok: true,
    range,
    data: (results || []).map((r: any) => ({
      day: r.day,
      orders: Number(r.orders) || 0,
      // REMOVED the /100 because your revenue is already in dollars (7193.43)
      revenue: Number(r.revenue) || 0 
    })),
  }, headers);
}

// =====================================================
// Public: Track page view
// POST /api/track
// Body: { referrer?, path? }
// =====================================================
if (req.method === "POST" && path.endsWith("/track")) {
  const body = await req.json().catch(() => ({})) as any;
  const pagePath = String(body.path || "/").slice(0, 500);
  const referrer = String(body.referrer || "").slice(0, 1000);
  const ua = req.headers.get("User-Agent") || "";
  const ip = req.headers.get("CF-Connecting-IP") || "";
  const country = req.headers.get("CF-IPCountry") || "";
  const city = req.headers.get("CF-IPCity") || "";

  // Determine source from referrer (filter out same-site referrers)
  let source = "direct";
  if (referrer) {
    const ref = referrer.toLowerCase();
    // Ignore same-site referrers (SPA navigation)
    const isSameSite = ref.includes("fur-fusion.com") || ref.includes("furfusion") || ref.includes("lovableproject.com") || ref.includes("lovable.app") || ref.includes("localhost");
    if (!isSameSite) {
      if (ref.includes("google") || ref.includes("bing") || ref.includes("yahoo") || ref.includes("duckduckgo") || ref.includes("baidu")) {
        source = "search";
      } else if (ref.includes("facebook") || ref.includes("instagram") || ref.includes("tiktok") || ref.includes("twitter") || ref.includes("youtube") || ref.includes("pinterest") || ref.includes("snapchat") || ref.includes("reddit") || ref.includes("t.co")) {
        source = "social";
      } else {
        source = "referral";
      }
    }
  }

  // Generate a simple session hash from IP + UA + date (privacy-friendly, no cookies)
  const dayStr = new Date().toISOString().slice(0, 10);
  const sessionRaw = `${ip}|${ua}|${dayStr}`;
  const sessionHash = await hashString(sessionRaw);

  try {
    await env.DB.prepare(
      `INSERT INTO page_views (path, referrer, source, country, city, session_hash, created_at)
       VALUES (?, ?, ?, ?, ?, ?, datetime('now'))`
    ).bind(pagePath, referrer, source, country, city, sessionHash).run();
  } catch (e: any) {
    // Table might not exist yet - create it
    if (e.message?.includes("no such table")) {
      await env.DB.prepare(`
        CREATE TABLE IF NOT EXISTS page_views (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          path TEXT NOT NULL DEFAULT '/',
          referrer TEXT DEFAULT '',
          source TEXT DEFAULT 'direct',
          country TEXT DEFAULT '',
          city TEXT DEFAULT '',
          session_hash TEXT DEFAULT '',
          created_at TEXT DEFAULT (datetime('now'))
        )
      `).run();
      await env.DB.prepare(
        `INSERT INTO page_views (path, referrer, source, country, city, session_hash, created_at)
         VALUES (?, ?, ?, ?, ?, ?, datetime('now'))`
      ).bind(pagePath, referrer, source, country, city, sessionHash).run();
    }
  }

  return json({ ok: true }, headers);
}

// =====================================================
// Admin: Analytics – Visitors & Page Views
// GET /api/admin/analytics/visitors?range=7d
// =====================================================
if (req.method === "GET" && path.endsWith("/analytics/visitors")) {
  const email =
    req.headers.get("Cf-Access-Authenticated-User-Email") ||
    req.headers.get("cf-access-authenticated-user-email");
  if (!email) return json({ ok: false, error: "Unauthorized" }, headers, 401);

  const range = url.searchParams.get("range") || "7d";
  let daysLimit: number | null = null;
  switch (range) {
    case "today": daysLimit = 1; break;
    case "7d":    daysLimit = 7; break;
    case "30d":   daysLimit = 30; break;
    case "90d":   daysLimit = 90; break;
    case "180d":  daysLimit = 180; break;
    case "all":   daysLimit = null; break;
    default: return json({ ok: false, error: "Invalid range" }, headers, 400);
  }

  const dateFilter = daysLimit
    ? `WHERE datetime(created_at) >= datetime('now', '-${daysLimit} days')`
    : "";

  try {
    // Daily breakdown
    const { results: daily } = await env.DB.prepare(`
      SELECT DATE(created_at) as day,
             COUNT(*) as page_views,
             COUNT(DISTINCT session_hash) as sessions
      FROM page_views
      ${dateFilter}
      GROUP BY day ORDER BY day ASC
    `).all();

    // By source
    const { results: bySrc } = await env.DB.prepare(`
      SELECT source, COUNT(*) as views, COUNT(DISTINCT session_hash) as sessions
      FROM page_views
      ${dateFilter}
      GROUP BY source ORDER BY views DESC
    `).all();

    // By country
    const { results: byCountry } = await env.DB.prepare(`
      SELECT country, COUNT(*) as views, COUNT(DISTINCT session_hash) as sessions
      FROM page_views
      ${dateFilter}
      GROUP BY country ORDER BY views DESC LIMIT 20
    `).all();

    // By city
    const { results: byCity } = await env.DB.prepare(`
      SELECT city, country, COUNT(*) as views, COUNT(DISTINCT session_hash) as sessions
      FROM page_views
      ${dateFilter}
      GROUP BY city, country ORDER BY views DESC LIMIT 20
    `).all();

    return json({
      ok: true,
      range,
      daily: (daily || []).map((r: any) => ({
        day: r.day,
        page_views: Number(r.page_views) || 0,
        sessions: Number(r.sessions) || 0,
      })),
      by_source: (bySrc || []).map((r: any) => ({
        source: r.source || "direct",
        views: Number(r.views) || 0,
        sessions: Number(r.sessions) || 0,
      })),
      by_country: (byCountry || []).map((r: any) => ({
        country: r.country || "Unknown",
        views: Number(r.views) || 0,
        sessions: Number(r.sessions) || 0,
      })),
      by_city: (byCity || []).map((r: any) => ({
        city: r.city || "Unknown",
        country: r.country || "",
        views: Number(r.views) || 0,
        sessions: Number(r.sessions) || 0,
      })),
    }, headers);
  } catch (e: any) {
    // Table might not exist yet
    return json({
      ok: true,
      range,
      daily: [],
      by_source: [],
      by_country: [],
      by_city: [],
    }, headers);
  }
}

      // =====================================================
      // ✅ Public: GET reviews summary (ALL approved reviews)
      // GET /api/reviews/summary
      // - This MUST NOT depend on the "loaded reviews"
      // =====================================================
      if (req.method === "GET" && path === "/api/reviews/summary") {
        const totalRow = await env.DB.prepare(
          `SELECT COUNT(*) as total, AVG(rating) as avg
           FROM reviews
           WHERE status='approved'`
        ).first();

        const { results } = await env.DB.prepare(
          `SELECT rating, COUNT(*) as c
           FROM reviews
           WHERE status='approved'
           GROUP BY rating`
        ).all();

        const breakdown: Record<1 | 2 | 3 | 4 | 5, number> = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };

        for (const row of results || []) {
          const r = Number((row as any).rating);
          const c = Number((row as any).c) || 0;
          if (r >= 1 && r <= 5) breakdown[r as 1 | 2 | 3 | 4 | 5] = c;
        }

        return json(
          {
            ok: true,
            total: Number((totalRow as any)?.total) || 0,
            average: Number((totalRow as any)?.avg) || 0,
            breakdown,
          },
          headers
        );
      }

      // =====================================================
      // ✅ Public: GET approved reviews (PAGINATED)
      // GET /api/reviews?limit=10&offset=0&rating=5
      // - Default limit: 10
      // - Max limit: 50 (safe)
      // =====================================================
      if (req.method === "GET" && path === "/api/reviews") {
        const limit = clampInt(url.searchParams.get("limit"), 10, 1, 50);
        const offset = clampInt(url.searchParams.get("offset"), 0, 0, 10000000);
        const rating = clampInt(url.searchParams.get("rating"), 0, 0, 5);

        let sql = `
          SELECT id, name, rating, text, created_at, images_json
          FROM reviews
          WHERE status='approved'
        `;
        const params: any[] = [];

        if (rating >= 1 && rating <= 5) {
          sql += " AND rating = ?";
          params.push(rating);
        }

        sql += " ORDER BY datetime(created_at) DESC LIMIT ? OFFSET ?";
        params.push(limit, offset);

        const { results } = await env.DB.prepare(sql).bind(...params).all();

        return json(
          {
            ok: true,
            reviews: (results || []).map((r: any) => ({
              id: String(r.id),
              author: String(r.name || ""),
              rating: Number(r.rating) || 5,
              text: String(r.text || ""),
              verified: true,
              created_at: r.created_at,
              images: safeJsonArray(r.images_json),
            })),
          },
          headers
        );
      }

      // =====================================================
      // Public: POST submit review (website form)
      // POST /api/reviews
      // body: { order_id,email,name,rating,text }
      // NOTE: no uploads here
      // =====================================================
      if (req.method === "POST" && path === "/api/reviews") {
        if (!requireJson(req)) {
          return json({ ok: false, error: "Content-Type must be application/json" }, headers, 400);
        }

        const body = await req.json().catch(() => null);
        if (!body) return json({ ok: false, error: "Invalid JSON body" }, headers, 400);

        const order_id = String(body.order_id || "").trim();
        const email = String(body.email || "").trim().toLowerCase();
        const name = String(body.name || "").trim();
        const rating = clampInt(body.rating, 0, 1, 5);
        const text = String(body.text || "").trim();
        const images_json = "[]";

        if (!order_id || !email || !name || !rating) {
          return json(
            { ok: false, error: "Missing required fields (order_id, email, name, rating)" },
            headers,
            400
          );
        }

        const order = await env.DB.prepare(
          `SELECT email, payment_status, fulfillment_status
           FROM orders
           WHERE order_id = ?`
        )
          .bind(order_id)
          .first();

        if (!order) return json({ ok: false, error: "Invalid Order ID" }, headers, 400);
        if (String(order.email || "").toLowerCase() !== email) {
          return json({ ok: false, error: "Email does not match this Order ID" }, headers, 400);
        }
        if (String(order.payment_status || "") !== "paid") {
          return json({ ok: false, error: "Order not paid" }, headers, 400);
        }
        if (
          !["approved", "fulfilled"].includes(
            String(order.fulfillment_status || "").toLowerCase()
          )
        ) {
          return json({ ok: false, error: "Order not approved for reviews yet" }, headers, 400);
        }

        const existing = await env.DB.prepare(`SELECT id FROM reviews WHERE order_id = ?`)
          .bind(order_id)
          .first();

        if (existing) {
          return json({ ok: false, error: "Review already exists for this Order ID" }, headers, 409);
        }

        await env.DB.prepare(
          `INSERT INTO reviews (order_id, email, name, rating, text, status, images_json)
           VALUES (?, ?, ?, ?, ?, 'pending', ?)`
        )
          .bind(order_id, email, name, rating, text, images_json)
          .run();

        return json({ ok: true, message: "Review submitted for approval" }, headers);
      }
// =====================================================
// Public: GET reviews summary (ALL approved reviews)
// GET /api/reviews/summary
// =====================================================
if (req.method === "GET" && path === "/api/reviews/summary") {
  const totalRow = await env.DB.prepare(
    `SELECT COUNT(*) as total, AVG(rating) as avg
     FROM reviews
     WHERE status='approved'`
  ).first();

  const { results } = await env.DB.prepare(
    `SELECT rating, COUNT(*) as c
     FROM reviews
     WHERE status='approved'
     GROUP BY rating`
  ).all();

  const breakdown: Record<1 | 2 | 3 | 4 | 5, number> = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };

  for (const row of results || []) {
    const r = Number((row as any).rating);
    const c = Number((row as any).c) || 0;
    if (r >= 1 && r <= 5) breakdown[r as 1 | 2 | 3 | 4 | 5] = c;
  }

  return json(
    {
      ok: true,
      total: Number((totalRow as any)?.total) || 0,
      average: Number((totalRow as any)?.avg) || 0,
      breakdown,
    },
    { ...headers, "Cache-Control": "no-store" }
  );
          }
      
      // =====================================================
      // Public: GET order
      // GET /api/orders/:order_id
      // =====================================================
      if (req.method === "GET" && path.startsWith("/api/orders/")) {
        const id = decodeURIComponent(path.split("/").pop() || "").trim();
        if (!id) return json({ ok: false, error: "Missing order_id" }, headers, 400);

        const order = await env.DB.prepare(
  `SELECT
     order_id,
     email,
     payment_status,
     fulfillment_status,
     tracking_number,
     quantity,
     created_at
   FROM orders
   WHERE order_id = ?`
)
  .bind(id)
  .first();


        if (!order) return json({ ok: false, error: "Not found" }, headers, 404);
        return json({ ok: true, order }, headers);
      }

      // =====================================================
      // =====================================================
// Public: POST Stripe Checkout
// POST /api/checkout
// =====================================================
if (req.method === "POST" && path === "/api/checkout") {
  if (!requireJson(req)) {
    return json({ ok: false, error: "Content-Type must be application/json" }, headers, 400);
  }

  const body = await req.json().catch(() => null);
  if (!body) return json({ ok: false, error: "Invalid JSON body" }, headers, 400);

  // ✅ quantity coming from frontend
  const rawQty = body.quantity;
  const quantity = clampInt(rawQty, 1, 1, 10);
  console.log("[CHECKOUT] body.quantity:", rawQty, "→ clamped:", quantity);

  const order_id = await generateUniqueOrderId(env.DB);

  // placeholder email until Stripe checkout completes
  const placeholderEmail = `pending+${order_id}@furfusion.local`;

  // ✅ IMPORTANT: insert quantity in DB at creation time
  console.log("[CHECKOUT] Inserting order:", order_id, "with quantity:", quantity, "type:", typeof quantity);
  const insertResult = await env.DB.prepare(
    `INSERT INTO orders (order_id, email, payment_status, fulfillment_status, quantity)
     VALUES (?, ?, 'unpaid', 'pending', ?)`
  )
    .bind(order_id, placeholderEmail, quantity)
    .run();
  console.log("[CHECKOUT] Insert result:", JSON.stringify(insertResult));
  
  // Verify the insert
  const verifyRow = await env.DB.prepare(`SELECT quantity FROM orders WHERE order_id = ?`).bind(order_id).first();
  console.log("[CHECKOUT] Verified quantity after insert:", verifyRow?.quantity);

  const session = await createStripeCheckoutSession(env, { order_id, quantity });
  console.log("[CHECKOUT] Stripe session created, metadata:", session?.metadata);

  if (session?.id) {
    await env.DB.prepare(`UPDATE orders SET stripe_session_id=? WHERE order_id=?`)
      .bind(String(session.id), String(order_id))
      .run()
      .catch(() => {});
  }

  return json({ ok: true, order_id, checkout_url: session.url }, headers);
}


      // =====================================================
// Stripe webhook
// POST /api/stripe/webhook
// =====================================================
if (req.method === "POST" && path === "/api/stripe/webhook") {
  const sig = req.headers.get("Stripe-Signature") || "";
  const raw = await req.text();

  const okSig = await verifyStripeWebhookRaw(env, sig, raw);
  if (!okSig) return json({ ok: false, error: "Invalid signature" }, headers, 400);

  const event = JSON.parse(raw || "{}") as any;
  console.log("[WEBHOOK] Event type:", event?.type);

  // We only need this one for orders
  if (event?.type === "checkout.session.completed") {
    const session = event.data?.object || {};

    const order_id = String(session?.metadata?.order_id || "").trim();
    console.log("[WEBHOOK] order_id from metadata:", order_id);
    console.log("[WEBHOOK] session.metadata:", JSON.stringify(session?.metadata));
    
    if (!order_id) return json({ ok: true }, headers); // ignore unknown

    // ✅ quantity: ALWAYS fetch from line_items (most reliable source of truth)
    // metadata is a backup, but line_items shows what was actually charged
    let quantity = 0;
    
    // First try line_items from Stripe (authoritative)
    console.log("[WEBHOOK] Fetching line items from Stripe...");
    try {
      const liRes = await fetch(
        `https://api.stripe.com/v1/checkout/sessions/${encodeURIComponent(
          session.id
        )}/line_items?limit=10`,
        {
          method: "GET",
          headers: { Authorization: `Bearer ${env.STRIPE_SECRET_KEY}` },
        }
      );
      const li = await liRes.json().catch(() => ({}));
      console.log("[WEBHOOK] Line items response:", JSON.stringify(li?.data?.[0]));
      const q = Number(li?.data?.[0]?.quantity || 0);
      if (Number.isFinite(q) && q > 0) quantity = q;
      console.log("[WEBHOOK] quantity from line_items:", q);
    } catch (e) {
      console.log("[WEBHOOK] Error fetching line items:", e);
    }
    
    // Fallback to metadata if line_items failed
    if (!Number.isFinite(quantity) || quantity <= 0) {
      const metaQty = Number(session?.metadata?.quantity || 0);
      console.log("[WEBHOOK] Fallback to metadata quantity:", session?.metadata?.quantity, "→ parsed:", metaQty);
      if (Number.isFinite(metaQty) && metaQty > 0) quantity = metaQty;
    }

    const amount_total = Number(session?.amount_total || 0);
    const payment_intent = String(session?.payment_intent || "");
    const customer = String(session?.customer || "");
    const email =
      String(session?.customer_details?.email || session?.customer_email || "").toLowerCase() || null;

    const quantityToUpdate = quantity > 0 ? quantity : null;
    console.log("[WEBHOOK] Updating order:", order_id, "with quantity:", quantityToUpdate, "(null means keep existing)");

    // ✅ IMPORTANT: update quantity, don't default to 1
    await env.DB.prepare(
      `UPDATE orders
       SET
         payment_status='paid',
         stripe_session_id=?,
         stripe_payment_intent_id=?,
         stripe_customer_id=?,
         total=?,
         email=COALESCE(?, email),
         quantity=COALESCE(?, quantity)
       WHERE order_id=?`
    )
      .bind(
        String(session.id || ""),
        payment_intent,
        customer,
        amount_total,
        email,
        quantityToUpdate,
        order_id
      )
      .run();
  }

  return json({ ok: true }, headers);
}
      // =====================================================
      // BLOGS (Public + Admin) — SINGLE CLEAN VERSION
      // =====================================================

      // Public: list published blogs
      // GET /api/blogs?limit=10&offset=0
      if (req.method === "GET" && path === "/api/blogs") {
        const limit = clampInt(url.searchParams.get("limit"), 10, 1, 50);
        const offset = clampInt(url.searchParams.get("offset"), 0, 0, 100000);

        const { results } = await env.DB.prepare(
          `SELECT id, slug, title, content, created_at
           FROM blog_posts
           WHERE is_published = 1
           ORDER BY datetime(created_at) DESC
           LIMIT ? OFFSET ?`
        )
          .bind(limit, offset)
          .all();

        const posts = (results || []).map((r: any) => {
          const c = normalizeBlogContent(r.content);
          return {
            id: Number(r.id),
            slug: String(r.slug),
            title: String(r.title),
            excerpt: c.excerpt || "",
            coverImage: c.coverImage || "",
            coverAlt: c.coverAlt || "",
            created_at: r.created_at,
          };
        });

        return json({ ok: true, posts }, headers);
      }

      // Public: single published blog by slug
      // GET /api/blogs/:slug
      if (req.method === "GET" && path.startsWith("/api/blogs/")) {
        const slug = decodeURIComponent(path.split("/").pop() || "").trim();
        if (!slug) return json({ ok: false, error: "Missing slug" }, headers, 400);

        const row = await env.DB.prepare(
          `SELECT id, slug, title, content, created_at
           FROM blog_posts
           WHERE slug = ? AND is_published = 1`
        )
          .bind(slug)
          .first();

        if (!row) return json({ ok: false, error: "Blog not found" }, headers, 404);

        const content = normalizeBlogContent((row as any).content);

        return json(
          {
            ok: true,
            post: {
              id: Number((row as any).id),
              slug: String((row as any).slug),
              title: String((row as any).title),
              content,
              created_at: (row as any).created_at,
            },
          },
          headers
        );
      }

      // Admin: list blog posts
      // GET /api/admin/blog-posts?limit=20&offset=0&q=
      if (req.method === "GET" && path === "/api/admin/blog-posts") {
        if (!isAdmin(req, env)) return json({ ok: false, error: "Unauthorized" }, headers, 401);

        const limit = clampInt(url.searchParams.get("limit"), 20, 1, 50);
        const offset = clampInt(url.searchParams.get("offset"), 0, 0, 100000);
        const q = String(url.searchParams.get("q") || "").trim().toLowerCase();

        let sql = `
          SELECT id, slug, title, content, is_published, created_at
          FROM blog_posts
          WHERE 1=1
        `;
        const params: any[] = [];

        if (q) {
          sql += ` AND (lower(title) LIKE ? OR lower(slug) LIKE ?)`;
          params.push(`%${q}%`, `%${q}%`);
        }

        sql += ` ORDER BY datetime(created_at) DESC LIMIT ? OFFSET ?`;
        params.push(limit, offset);

        const { results } = await env.DB.prepare(sql).bind(...params).all();
        return json({ ok: true, posts: results || [] }, headers);
      }

      // Admin: create blog post
      // POST /api/admin/blog-posts
      if (req.method === "POST" && path === "/api/admin/blog-posts") {
        if (!isAdmin(req, env)) return json({ ok: false, error: "Unauthorized" }, headers, 401);
        if (!requireJson(req)) {
          return json({ ok: false, error: "Content-Type must be application/json" }, headers, 400);
        }

        const body = await req.json().catch(() => null);
        if (!body) return json({ ok: false, error: "Invalid JSON body" }, headers, 400);

        const slug = String(body.slug || "").trim().toLowerCase();
        const title = String(body.title || "").trim();
        const is_published = body.is_published ? 1 : 0;

        const contentObj = normalizeBlogContent(body.content ?? {});
        const content = JSON.stringify(contentObj);

        if (!slug || !title)
          return json({ ok: false, error: "slug and title required" }, headers, 400);
        if (!contentObj.blocks.length) {
          return json({ ok: false, error: "content.blocks is required" }, headers, 400);
        }

        try {
          const res = await env.DB.prepare(
            `INSERT INTO blog_posts (slug, title, content, is_published)
             VALUES (?, ?, ?, ?)`
          )
            .bind(slug, title, content, is_published)
            .run();

          return json({ ok: true, id: (res as any).meta?.last_row_id ?? null }, headers);
        } catch (e: any) {
          return json({ ok: false, error: String(e?.message || e) }, headers, 400);
        }
      }

      // Admin: update blog post
      // PATCH /api/admin/blog-posts/:id
      if (req.method === "PATCH" && path.startsWith("/api/admin/blog-posts/")) {
        if (!isAdmin(req, env)) return json({ ok: false, error: "Unauthorized" }, headers, 401);
        if (!requireJson(req)) {
          return json({ ok: false, error: "Content-Type must be application/json" }, headers, 400);
        }

        const idRaw = decodeURIComponent(path.split("/").pop() || "").trim();
        const id = Number(idRaw);
        if (!Number.isFinite(id) || id <= 0) {
          return json({ ok: false, error: "Invalid blog id" }, headers, 400);
        }

        const body = await req.json().catch(() => null);
        if (!body) return json({ ok: false, error: "Invalid JSON body" }, headers, 400);

        const fields: string[] = [];
        const params: any[] = [];

        if (typeof body.slug === "string" && body.slug.trim()) {
          fields.push("slug=?");
          params.push(body.slug.trim().toLowerCase());
        }
        if (typeof body.title === "string" && body.title.trim()) {
          fields.push("title=?");
          params.push(body.title.trim());
        }
        if (body.content !== undefined) {
          const contentObj = normalizeBlogContent(body.content);
          fields.push("content=?");
          params.push(JSON.stringify(contentObj));
        }
        if (body.is_published !== undefined) {
          fields.push("is_published=?");
          params.push(body.is_published ? 1 : 0);
        }

        if (!fields.length)
          return json({ ok: false, error: "No fields to update" }, headers, 400);

        params.push(id);

        await env.DB.prepare(`UPDATE blog_posts SET ${fields.join(", ")} WHERE id=?`)
          .bind(...params)
          .run();

        return json({ ok: true }, headers);
      }

      // Admin: delete blog post
      // DELETE /api/admin/blog-posts/:id
      if (req.method === "DELETE" && path.startsWith("/api/admin/blog-posts/")) {
        if (!isAdmin(req, env)) return json({ ok: false, error: "Unauthorized" }, headers, 401);

        const idRaw = decodeURIComponent(path.split("/").pop() || "").trim();
        const id = Number(idRaw);
        if (!Number.isFinite(id) || id <= 0) {
          return json({ ok: false, error: "Invalid blog id" }, headers, 400);
        }

        await env.DB.prepare(`DELETE FROM blog_posts WHERE id=?`).bind(id).run();
        return json({ ok: true }, headers);
      }

      // =====================================================
      // Admin BLOGS aliases (so old frontend keeps working)
      // /api/admin/blogs  == /api/admin/blog-posts
      // =====================================================

      // GET list (draft + published)
      if (req.method === "GET" && path === "/api/admin/blogs") {
        if (!isAdmin(req, env)) return json({ ok: false, error: "Unauthorized" }, headers, 401);

        const limit = clampInt(url.searchParams.get("limit"), 20, 1, 50);
        const offset = clampInt(url.searchParams.get("offset"), 0, 0, 100000);
        const q = String(url.searchParams.get("q") || "").trim().toLowerCase();

        let sql = `
          SELECT id, slug, title, content, is_published, created_at
          FROM blog_posts
          WHERE 1=1
        `;
        const params: any[] = [];

        if (q) {
          sql += ` AND (lower(title) LIKE ? OR lower(slug) LIKE ?)`;
          params.push(`%${q}%`, `%${q}%`);
        }

        sql += ` ORDER BY datetime(created_at) DESC LIMIT ? OFFSET ?`;
        params.push(limit, offset);

        const { results } = await env.DB.prepare(sql).bind(...params).all();
        return json({ ok: true, posts: results || [] }, headers);
      }

      // POST create
      if (req.method === "POST" && path === "/api/admin/blogs") {
        if (!isAdmin(req, env)) return json({ ok: false, error: "Unauthorized" }, headers, 401);
        if (!requireJson(req))
          return json({ ok: false, error: "Content-Type must be application/json" }, headers, 400);

        const body = await req.json().catch(() => null);
        if (!body) return json({ ok: false, error: "Invalid JSON body" }, headers, 400);

        const slug = String(body.slug || "").trim().toLowerCase();
        const title = String(body.title || "").trim();
        const is_published = body.is_published ? 1 : 0;

        const contentObj = normalizeBlogContent(body.content ?? {});
        const content = JSON.stringify(contentObj);

        if (!slug || !title)
          return json({ ok: false, error: "slug and title required" }, headers, 400);
        if (!contentObj.blocks.length)
          return json({ ok: false, error: "content.blocks is required" }, headers, 400);

        try {
          const res = await env.DB.prepare(
            `INSERT INTO blog_posts (slug, title, content, is_published)
             VALUES (?, ?, ?, ?)`
          )
            .bind(slug, title, content, is_published)
            .run();

          return json({ ok: true, id: (res as any).meta?.last_row_id ?? null }, headers);
        } catch (e: any) {
          return json({ ok: false, error: String(e?.message || e) }, headers, 400);
        }
      }

      // PATCH / DELETE by id
      if (
        (req.method === "PATCH" || req.method === "DELETE") &&
        path.startsWith("/api/admin/blogs/")
      ) {
        if (!isAdmin(req, env)) return json({ ok: false, error: "Unauthorized" }, headers, 401);

        const idRaw = decodeURIComponent(path.split("/").pop() || "").trim();
        const id = Number(idRaw);
        if (!Number.isFinite(id) || id <= 0)
          return json({ ok: false, error: "Invalid blog id" }, headers, 400);

        if (req.method === "DELETE") {
          await env.DB.prepare(`DELETE FROM blog_posts WHERE id=?`).bind(id).run();
          return json({ ok: true }, headers);
        }

        // PATCH
        if (!requireJson(req))
          return json({ ok: false, error: "Content-Type must be application/json" }, headers, 400);

        const body = await req.json().catch(() => null);
        if (!body) return json({ ok: false, error: "Invalid JSON body" }, headers, 400);

        const fields: string[] = [];
        const params: any[] = [];

        if (typeof body.slug === "string" && body.slug.trim()) {
          fields.push("slug=?");
          params.push(body.slug.trim().toLowerCase());
        }
        if (typeof body.title === "string" && body.title.trim()) {
          fields.push("title=?");
          params.push(body.title.trim());
        }
        if (body.content !== undefined) {
          const contentObj = normalizeBlogContent(body.content);
          fields.push("content=?");
          params.push(JSON.stringify(contentObj));
        }
        if (body.is_published !== undefined) {
          fields.push("is_published=?");
          params.push(body.is_published ? 1 : 0);
        }

        if (!fields.length)
          return json({ ok: false, error: "No fields to update" }, headers, 400);

        params.push(id);

        await env.DB.prepare(`UPDATE blog_posts SET ${fields.join(", ")} WHERE id=?`)
          .bind(...params)
          .run();

        return json({ ok: true }, headers);
      }

      // ===== END PART 1/3 =====
      // Part 2 starts with: Admin Orders/Reviews/Fulfill/Refund/Review-Request/Contact/Cron + Not found
      return json({ ok: false, error: "Not found" }, headers, 404);
    } catch (e: any) {
      return json({ ok: false, error: String(e?.message || e) }, headers, 500);
    }
  },
};

// =====================================================
// PART 2/3
// Admin Orders / Reviews / Fulfillment / Refunds
// Review Requests (manual + cron)
// Contact form
// Not Found
// =====================================================
try {
// =====================================================
// Admin: GET orders (paid only)
// GET /api/admin/orders?limit=20&offset=0&q=...
// =====================================================
if (req.method === "GET" && path === "/api/admin/orders") {
  if (!isAdmin(req, env)) return json({ ok: false, error: "Unauthorized" }, headers, 401);

  const limit = clampInt(url.searchParams.get("limit"), 20, 1, 50);
  const offset = clampInt(url.searchParams.get("offset"), 0, 0, 100000);
  const q = String(url.searchParams.get("q") || "").trim().toLowerCase();

  let sql = `
    SELECT
      order_id, email, full_name, phone,
      address1, address2, city, state, zip, country,
      billing_address1, billing_address2, billing_city,
      billing_state, billing_zip, billing_country,
      payment_status, fulfillment_status,
      tracking_number, total, quantity, created_at,
      invoice_status, invoice_sent_at,
      stripe_payment_intent_id,
      stripe_invoice_pdf_url, stripe_hosted_invoice_url,
      approved_at, review_request_sent_at,
      review_request_disabled
    FROM orders
    WHERE payment_status='paid'
  `;
  const params: any[] = [];

  if (q) {
    sql += ` AND (lower(order_id) LIKE ? OR lower(email) LIKE ?)`;
    params.push(`%${q}%`, `%${q}%`);
  }

  sql += ` ORDER BY datetime(created_at) DESC LIMIT ? OFFSET ?`;
  params.push(limit, offset);

  const { results } = await env.DB.prepare(sql).bind(...params).all();
  return json({ ok: true, orders: results || [] }, headers);
}

// =====================================================
// Admin: GET reviews
// GET /api/admin/reviews?limit=20&offset=0&status=&q=
// =====================================================
if (req.method === "GET" && path === "/api/admin/reviews") {
  if (!isAdmin(req, env)) return json({ ok: false, error: "Unauthorized" }, headers, 401);

  const limit = clampInt(url.searchParams.get("limit"), 20, 1, 50);
  const offset = clampInt(url.searchParams.get("offset"), 0, 0, 100000);
  const status = String(url.searchParams.get("status") || "").trim().toLowerCase();
  const q = String(url.searchParams.get("q") || "").trim().toLowerCase();

  let sql = `
    SELECT id, order_id, email, name, rating, text, status, created_at, images_json
    FROM reviews
  `;
  const where: string[] = [];
  const params: any[] = [];

  if (status && ["pending", "approved", "rejected"].includes(status)) {
    where.push("status=?");
    params.push(status);
  }

  if (q) {
    where.push(
      "(lower(order_id) LIKE ? OR lower(email) LIKE ? OR lower(name) LIKE ? OR lower(text) LIKE ?)"
    );
    params.push(`%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`);
  }

  if (where.length) sql += ` WHERE ${where.join(" AND ")}`;
  sql += ` ORDER BY datetime(created_at) DESC LIMIT ? OFFSET ?`;
  params.push(limit, offset);

  const { results } = await env.DB.prepare(sql).bind(...params).all();

  const rows = (results || []).map((r: any) => ({
    ...r,
    images: safeJsonArray(r.images_json),
  }));

  return json({ ok: true, reviews: rows }, headers);
}

// =====================================================
// Admin: PATCH review status
// PATCH /api/admin/reviews/:id
// =====================================================
if (req.method === "PATCH" && path.startsWith("/api/admin/reviews/")) {
  if (!isAdmin(req, env)) return json({ ok: false, error: "Unauthorized" }, headers, 401);
  if (!requireJson(req))
    return json({ ok: false, error: "Content-Type must be application/json" }, headers, 400);

  const idRaw = decodeURIComponent(path.split("/").pop() || "").trim();
  const id = Number(idRaw);
  if (!Number.isFinite(id) || id <= 0)
    return json({ ok: false, error: "Invalid review id" }, headers, 400);

  const body = await req.json().catch(() => null);
  if (!body) return json({ ok: false, error: "Invalid JSON body" }, headers, 400);

  const status = String(body.status || "").trim().toLowerCase();
  if (!["approved", "rejected"].includes(status))
    return json({ ok: false, error: "Invalid status" }, headers, 400);

  const review = await env.DB.prepare(
    `SELECT email, name, status FROM reviews WHERE id=?`
  )
    .bind(id)
    .first();

  if (!review) return json({ ok: false, error: "Review not found" }, headers, 404);

  await env.DB.prepare(`UPDATE reviews SET status=? WHERE id=?`)
    .bind(status, id)
    .run();

  if (
    status === "approved" &&
    env.RESEND_API_KEY &&
    env.FROM_EMAIL &&
    review.email &&
    String(review.status).toLowerCase() !== "approved"
  ) {
    await resendSendReviewApprovedEmail(env, {
      to: String(review.email),
      name: String(review.name || "Customer"),
    }).catch(() => {});
  }

  return json({ ok: true }, headers);
}

// =====================================================
// Admin: POST fulfill order
// POST /api/admin/fulfill
// =====================================================
if (req.method === "POST" && path === "/api/admin/fulfill") {
  if (!isAdmin(req, env)) return json({ ok: false, error: "Unauthorized" }, headers, 401);
  if (!requireJson(req))
    return json({ ok: false, error: "Content-Type must be application/json" }, headers, 400);

  const body = await req.json().catch(() => null);
  if (!body) return json({ ok: false, error: "Invalid JSON body" }, headers, 400);

  const order_id = String(body.order_id || "").trim();
  const tracking_number = String(body.tracking_number || "").trim();
  const carrier = String(body.carrier || "").trim() || null;

  if (!order_id || !tracking_number)
    return json({ ok: false, error: "order_id and tracking_number required" }, headers, 400);

  await env.DB.prepare(
    `UPDATE orders
     SET tracking_number=?,
         fulfillment_status='approved',
         approved_at=COALESCE(approved_at, datetime('now'))
     WHERE order_id=?`
  )
    .bind(tracking_number, order_id)
    .run();

  return json({ ok: true }, headers);
}

// =====================================================
// Admin: POST refund
// POST /api/admin/refund
// =====================================================
if (req.method === "POST" && path === "/api/admin/refund") {
  if (!isAdmin(req, env)) return json({ ok: false, error: "Unauthorized" }, headers, 401);
  if (!requireJson(req))
    return json({ ok: false, error: "Content-Type must be application/json" }, headers, 400);

  const body = await req.json().catch(() => null);
  if (!body) return json({ ok: false, error: "Invalid JSON body" }, headers, 400);

  const order_id = String(body.order_id || "").trim();
  const amount_cents = clampInt(body.amount_cents, 0, 1, 999999999);

  if (!order_id || !amount_cents)
    return json({ ok: false, error: "order_id and amount_cents required" }, headers, 400);

  const order = await env.DB.prepare(
    `SELECT stripe_payment_intent_id FROM orders WHERE order_id=?`
  )
    .bind(order_id)
    .first();

  if (!order || !order.stripe_payment_intent_id)
    return json({ ok: false, error: "Order not refundable" }, headers, 400);

  await stripeCreateRefund(env, {
    payment_intent: String(order.stripe_payment_intent_id),
    amount: amount_cents,
  });

  await env.DB.prepare(
    `UPDATE orders SET fulfillment_status='refunded' WHERE order_id=?`
  )
    .bind(order_id)
    .run();

  return json({ ok: true }, headers);
}

// =====================================================
// Admin: manual send review request
// POST /api/admin/send-review-request
// =====================================================
if (req.method === "POST" && path === "/api/admin/send-review-request") {
  if (!isAdmin(req, env)) return json({ ok: false, error: "Unauthorized" }, headers, 401);

  const body = await req.json().catch(() => null);
  if (!body) return json({ ok: false, error: "Invalid JSON body" }, headers, 400);

  const order_id = String(body.order_id || "").trim();
  if (!order_id) return json({ ok: false, error: "order_id required" }, headers, 400);

  const order = await env.DB.prepare(
    `SELECT email FROM orders WHERE order_id=?`
  )
    .bind(order_id)
    .first();

  if (!order || !order.email)
    return json({ ok: false, error: "Order email missing" }, headers, 400);

  const token = await createReviewToken(env, {
    order_id,
    email: String(order.email),
  });

  const reviewUrl =
    `${env.SITE_URL}/api/reviews/email-form?order_id=${encodeURIComponent(order_id)}&token=${encodeURIComponent(token)}`;

  await resendSendReviewRequestEmail(env, {
    to: String(order.email),
    order_id,
    reviewUrl,
  });

  return json({ ok: true }, headers);
}

// =====================================================
// Public: Contact form
// POST /api/contact
// =====================================================
if (req.method === "POST" && path === "/api/contact") {
  if (!requireJson(req))
    return json({ ok: false, error: "Content-Type must be application/json" }, headers, 400);

  const body = await req.json().catch(() => null);
  if (!body) return json({ ok: false, error: "Invalid JSON body" }, headers, 400);

  const name = String(body.name || "").trim();
  const email = String(body.email || "").trim().toLowerCase();
  const message = String(body.message || "").trim();

  if (!name || !email || !message)
    return json({ ok: false, error: "Missing fields" }, headers, 400);

  return json({ ok: true }, headers);
}


// =====================================================
// Not found
// =====================================================
return json({ ok: false, error: "Not found" }, headers, 404);
    } catch (e: any) {
      return json({ ok: false, error: String(e?.message || e) }, headers, 500);
    }
  },
};

// =====================================================
// Helpers
// =====================================================
function cors(req: Request) {
  const origin = req.headers.get("Origin") || "";

  const allowed = new Set([
    "https://fur-fusion.com",
    "https://www.fur-fusion.com",
    "http://localhost:8080",
    "http://127.0.0.1:8080",
  ]);

  const allowOrigin = allowed.has(origin) ? origin : "https://fur-fusion.com";

  return {
    "Access-Control-Allow-Origin": allowOrigin,
    "Access-Control-Allow-Headers":
      "Content-Type, Authorization, Stripe-Signature, CF-Access-Jwt-Assertion",
    "Access-Control-Allow-Methods": "GET, POST, PATCH, DELETE, OPTIONS",
    "Access-Control-Allow-Credentials": "true",
    Vary: "Origin",
  };
}

function json(data: any, headers: Record<string, string>, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
}

function requireJson(req: Request) {
  const ct = req.headers.get("Content-Type") || "";
  return ct.toLowerCase().includes("application/json");
}

function clampInt(v: any, fallback: number, min: number, max: number) {
  const n = typeof v === "string" ? parseInt(v, 10) : Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(Math.max(n, min), max);
}

function safeJsonArray(v: any): string[] {
  if (!v) return [];
  if (Array.isArray(v)) return v.map(String).filter(Boolean);
  try {
    const arr = JSON.parse(String(v));
    return Array.isArray(arr) ? arr.map(String).filter(Boolean) : [];
  } catch {
    return [];
  }
}

function getCookie(req: Request, name: string) {
  const cookie = req.headers.get("Cookie") || "";
  const parts = cookie.split(";").map((p) => p.trim());
  for (const p of parts) {
    if (p.startsWith(name + "=")) return p.slice(name.length + 1);
  }
  return null;
}

function b64urlDecodeToString(b64url: string) {
  const b64 =
    b64url.replace(/-/g, "+").replace(/_/g, "/") + "===".slice((b64url.length + 3) % 4);
  return atob(b64);
}

function getAccessEmail(req: Request): string | null {
  const h =
    req.headers.get("cf-access-authenticated-user-email") ||
    req.headers.get("Cf-Access-Authenticated-User-Email");
  if (h) return h.trim().toLowerCase();

  const jwt = getCookie(req, "CF_Authorization");
  if (!jwt) return null;

  const parts = jwt.split(".");
  if (parts.length < 2) return null;

  try {
    const payload = JSON.parse(b64urlDecodeToString(parts[1]));
    const email = String(payload?.email || "").trim().toLowerCase();
    return email || null;
  } catch {
    return null;
  }
}

function isAdmin(req: Request, env: Env): boolean {
  const email = getAccessEmail(req);
  if (!email) return false;

  const raw = String(env.ADMIN_EMAIL || "").toLowerCase().trim();
  if (!raw) return false;

  const allowed = raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  return allowed.includes(email);
}

async function generateUniqueOrderId(db: D1Database) {
  for (let i = 0; i < 10; i++) {
    const id = `FF-2026-${Math.floor(Math.random() * 900000) + 100000}`;
    const exists = await db.prepare(`SELECT id FROM orders WHERE order_id = ?`).bind(id).first();
    if (!exists) return id;
  }
  throw new Error("Could not generate order id");
}

// =====================================================
// Stripe: create checkout session
// =====================================================
async function createStripeCheckoutSession(
  env: Env,
  { order_id, quantity }: { order_id: string; quantity: number }
) {
  if (!env.STRIPE_SECRET_KEY) throw new Error("Missing STRIPE_SECRET_KEY");
  if (!env.SITE_URL) throw new Error("Missing SITE_URL");
  if (!env.STRIPE_PRICE_ID) throw new Error("Missing STRIPE_PRICE_ID");

  const body = new URLSearchParams();
  body.set("mode", "payment");
  body.set("success_url", `${env.SITE_URL}/success?order_id=${encodeURIComponent(order_id)}`);
  body.set("cancel_url", `${env.SITE_URL}/product`);

  body.set("shipping_address_collection[allowed_countries][0]", "US");
  body.set("billing_address_collection", "required");
  body.set("phone_number_collection[enabled]", "true");
  body.set("customer_creation", "always");

  body.set("invoice_creation[enabled]", "true");
  body.set("invoice_creation[invoice_data][description]", "FurFusion order");
  body.set("invoice_creation[invoice_data][footer]", "Thank you for shopping with FurFusion.");

  body.set("line_items[0][price]", env.STRIPE_PRICE_ID);
  body.set("line_items[0][quantity]", String(quantity));

  // ✅ IMPORTANT: store quantity in metadata for webhook backup
  body.set("metadata[order_id]", order_id);
  body.set("metadata[quantity]", String(quantity));

  const res = await fetch("https://api.stripe.com/v1/checkout/sessions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.STRIPE_SECRET_KEY}`,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body,
  });

  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error((data as any)?.error?.message || "Stripe session create failed");
  return data as any;
}

// =====================================================
// Stripe: webhook verify (HMAC SHA-256) - hex compare
// =====================================================
async function verifyStripeWebhookRaw(env: Env, stripeSignature: string, rawText: string) {
  if (!env.STRIPE_WEBHOOK_SECRET) throw new Error("Missing STRIPE_WEBHOOK_SECRET");

  const parts: Record<string, string> = {};
  for (const seg of (stripeSignature || "").split(",")) {
    const [k, v] = seg.split("=").map((s) => s.trim());
    if (k && v) parts[k] = v;
  }

  const t = parts.t;
  const v1 = parts.v1;
  if (!t || !v1) return false;

  const signedPayload = `${t}.${rawText}`;

  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(env.STRIPE_WEBHOOK_SECRET),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );

  const sigBuf = await crypto.subtle.sign("HMAC", key, enc.encode(signedPayload));
  const sigHex = [...new Uint8Array(sigBuf)].map((b) => b.toString(16).padStart(2, "0")).join("");

  if (sigHex.length !== v1.length) return false;
  let out = 0;
  for (let i = 0; i < sigHex.length; i++) out |= sigHex.charCodeAt(i) ^ v1.charCodeAt(i);
  return out === 0;
}

async function stripeRetrieveInvoice(env: Env, invoiceId: string) {
  const res = await fetch(`https://api.stripe.com/v1/invoices/${encodeURIComponent(invoiceId)}`, {
    method: "GET",
    headers: { Authorization: `Bearer ${env.STRIPE_SECRET_KEY}` },
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error((data as any)?.error?.message || "Stripe invoice retrieve failed");
  return data as any;
}

async function stripeCreateRefund(
  env: Env,
  args: { payment_intent: string; amount: number; reason?: string }
) {
  const body = new URLSearchParams();
  body.set("payment_intent", args.payment_intent);
  body.set("amount", String(args.amount));
  if (args.reason) body.set("reason", args.reason);

  const res = await fetch("https://api.stripe.com/v1/refunds", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.STRIPE_SECRET_KEY}`,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body,
  });

  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error((data as any)?.error?.message || "Stripe refund failed");
  return data as any;
}

// =====================================================
// EMAILS (brand + wrapper)
// =====================================================
function brand() {
  return {
    primary: "#4C2A39",
    accent: "#F67ACB",
    bg: "#f6f6f6",
    card: "#ffffff",
    text: "#111111",
    muted: "#555555",
    border: "#eaeaea",
    logo: "https://fur-fusion.com/favicon.png",
    name: "FurFusion",
    tagline: "where pets and quality merge",
    support: "support@fur-fusion.com",
    address: "271 W. Short St Ste 410, Lexington, KY 40507",
    website: "https://fur-fusion.com",
  };
}

function renderSignatureHtml(env: Env) {
  const b = brand();
  const logoUrl = env.LOGO_URL || b.logo;

  return `
    <tr>
      <td style="padding: 14px 24px 22px 24px;">
        <div style="border-top:1px solid ${b.border};padding-top:14px;">
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
            <tr>
              <td style="width:44px;vertical-align:top;">
                <img src="${escapeAttr(logoUrl)}" alt="${escapeAttr(b.name)}"
                     width="40" height="40"
                     style="display:block;border:0;border-radius:10px;">
              </td>
              <td style="padding-left:12px;vertical-align:top;">
                <div style="font-size:14px;font-weight:800;color:${b.text};line-height:1.2;">
                  ${escapeHtml(b.name)}
                </div>
                <div style="font-size:12px;color:${b.muted};margin-top:2px;">
                  ${escapeHtml(b.tagline)}
                </div>
                <div style="font-size:12px;color:${b.muted};margin-top:8px;line-height:1.6;">
                  Email: <a href="mailto:${escapeAttr(b.support)}" style="color:${b.primary};text-decoration:none;font-weight:700;">${escapeHtml(
                    b.support
                  )}</a><br/>
                  Address: ${escapeHtml(b.address)}<br/>
                  Website: <a href="${escapeAttr(b.website)}" style="color:${b.primary};text-decoration:none;font-weight:700;">fur-fusion.com</a>
                </div>
              </td>
            </tr>
          </table>
        </div>
      </td>
    </tr>
  `;
}

function emailWrapper(env: Env, args: { title: string; preheader: string; bodyHtml: string }) {
  const b = brand();
  const logoUrl = env.LOGO_URL || b.logo;

  return `<!doctype html>
<html>
  <head>
    <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
    <title>${escapeHtml(args.title)}</title>
    <style>
      .container { width:100%; background:${b.bg}; padding:16px 0; }
      .card { width:640px; max-width:640px; background:${b.card}; border-radius:18px; overflow:hidden; box-shadow:0 8px 24px rgba(0,0,0,.08); }
      .px { padding-left:24px; padding-right:24px; }
      .btn { display:inline-block; padding:12px 16px; border-radius:12px; text-decoration:none; font-weight:800; font-size:14px; }
      .btnPrimary { background:${b.primary}; color:#fff; }
      .btnGhost { background:#fff; border:1px solid ${b.border}; color:${b.text}; }
      .h1 { font-size:22px; font-weight:900; color:${b.text}; margin:0; }
      .p { font-size:14px; line-height:1.65; color:${b.muted}; margin:0; }
      .small { font-size:12px; line-height:1.6; color:#777; }
      @media (max-width: 700px) {
        .card { width:100% !important; border-radius:0 !important; }
        .px { padding-left:16px !important; padding-right:16px !important; }
      }
    </style>
  </head>
  <body style="margin:0;padding:0;">
    <div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent;">
      ${escapeHtml(args.preheader)}
    </div>

    <table role="presentation" class="container" width="100%" cellspacing="0" cellpadding="0" style="font-family:Arial, sans-serif;">
      <tr>
        <td align="center">
          <table role="presentation" class="card" width="640" cellspacing="0" cellpadding="0">
            <tr>
              <td style="background:${b.primary};padding:16px 24px;">
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
                  <tr>
                    <td>
                      <img src="${escapeAttr(logoUrl)}" alt="${escapeAttr(b.name)}" height="34" style="display:block;border:0;outline:none;">
                    </td>
                    <td align="right" style="color:#fff;font-weight:800;font-size:12px;">
                      ${escapeHtml(b.tagline)}
                    </td>
                  </tr>
                </table>
              </td>
            </tr>

            ${args.bodyHtml}

            ${renderSignatureHtml(env)}

            <tr>
              <td class="px" style="padding-bottom:18px;">
                <div class="small">
                  If you didn’t make this request, please reply to this email.
                </div>
              </td>
            </tr>

          </table>
        </td>
      </tr>
    </table>
  </body>
</html>`;
}

// =====================================================
// Resend: review approved email
// =====================================================
async function resendSendReviewApprovedEmail(env: Env, { to, name }: { to: string; name: string }) {
  if (!env.RESEND_API_KEY) throw new Error("Missing RESEND_API_KEY");
  if (!env.FROM_EMAIL) throw new Error("Missing FROM_EMAIL");

  const subject = "Your FurFusion review is now live ✅";
  const preheader = "Thanks for sharing your feedback — your review is now visible on our website.";

  const bodyHtml = `
    <tr>
      <td class="px" style="padding:22px 24px 6px 24px;">
        <div class="h1">Thanks, ${escapeHtml(name)}!</div>
        <div class="p" style="margin-top:8px;">
          Your review has been approved and is now visible on our website.
        </div>
      </td>
    </tr>

    <tr>
      <td class="px" style="padding:14px 24px 10px 24px;">
        <div style="border:1px solid #eaeaea;border-radius:14px;padding:16px;">
          <div style="margin-top:2px;">
            <a class="btn btnPrimary" href="${escapeAttr(brand().website + "/product#reviews")}">View Reviews</a>
          </div>
        </div>
      </td>
    </tr>
  `;

  const html = emailWrapper(env, { title: subject, preheader, bodyHtml });

  const res = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: { Authorization: `Bearer ${env.RESEND_API_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ from: env.FROM_EMAIL, to, subject, html }),
  });

  if (!res.ok) throw new Error(`Resend failed: ${res.status} ${await res.text().catch(() => "")}`);
}

// =====================================================
// Resend: review request email
// =====================================================
async function resendSendReviewRequestEmail(
  env: Env,
  { to, order_id, reviewUrl }: { to: string; order_id: string; reviewUrl: string }
) {
  if (!env.RESEND_API_KEY) throw new Error("Missing RESEND_API_KEY");
  if (!env.FROM_EMAIL) throw new Error("Missing FROM_EMAIL");

  const b = brand();
  const subject = `How are you liking your FurFusion order? (${order_id})`;
  const preheader = "Tap a star to rate your experience — it takes 10 seconds.";

  const bodyHtml = `
    <tr>
      <td class="px" style="padding:22px 24px 6px 24px;">
        <div class="h1">Quick question ❤️</div>
        <div class="p" style="margin-top:8px;">
          We hope your order arrived safely. How would you rate your experience?
        </div>
      </td>
    </tr>

    <tr>
      <td class="px" style="padding:10px 24px 10px 24px;">
        <div style="border:1px solid ${b.border};border-radius:14px;padding:16px;text-align:center;">
          <div style="font-size:13px;color:${b.muted};margin-bottom:10px;">
            Order ID: <b style="color:${b.text};">${escapeHtml(order_id)}</b>
          </div>

          <div style="margin:10px 0 6px 0; text-align:center;">
            <div style="font-size:0; line-height:0;">
              ${[1, 2, 3, 4, 5]
                .map((n) => {
                  const link = `${reviewUrl}&rating=${n}`;
                  return `
                    <a href="${escapeAttr(link)}"
                      style="display:inline-block; text-decoration:none; font-size:28px; line-height:28px;
                             margin:0 4px; color:${b.accent}; font-weight:900;">
                      ★
                    </a>
                  `;
                })
                .join("")}
            </div>
            <div style="margin-top:8px; font-size:12px; color:${b.muted};">
              Tap a star to rate (1–5)
            </div>
          </div>

          <div style="margin-top:14px;">
            <a class="btn btnPrimary" href="${escapeAttr(reviewUrl)}">Write a quick review</a>
          </div>

          <div style="margin-top:10px;font-size:12px;color:#777;line-height:1.5;">
            Your review will be submitted for approval and then published on our website.
          </div>
        </div>
      </td>
    </tr>
  `;

  const html = emailWrapper(env, { title: subject, preheader, bodyHtml });

  const res = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: { Authorization: `Bearer ${env.RESEND_API_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ from: env.FROM_EMAIL, to, subject, html }),
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`Resend failed: ${res.status} ${t}`);
  }
}

// =====================================================
// Review link token (signed token)
// token = base64url(JSON payload).hexHmac
// =====================================================
async function createReviewToken(env: Env, { order_id, email }: { order_id: string; email: string }) {
  if (!env.REVIEW_LINK_SECRET) throw new Error("Missing REVIEW_LINK_SECRET");

  const exp = Date.now() + 45 * 24 * 60 * 60 * 1000;
  const payload = { order_id, email: email.toLowerCase(), exp };
  const payloadStr = JSON.stringify(payload);
  const payloadB64 = base64urlEncode(payloadStr);

  const sigHex = await hmacHex(env.REVIEW_LINK_SECRET, payloadB64);
  return `${payloadB64}.${sigHex}`;
}

async function verifyReviewToken(env: Env, { token, order_id }: { token: string; order_id: string }) {
  if (!env.REVIEW_LINK_SECRET) return false;

  const [payloadB64, sigHex] = String(token || "").split(".");
  if (!payloadB64 || !sigHex) return false;

  const expected = await hmacHex(env.REVIEW_LINK_SECRET, payloadB64);
  if (!timingSafeEqualHex(expected, sigHex)) return false;

  let payload: any = null;
  try {
    payload = JSON.parse(base64urlDecode(payloadB64));
  } catch {
    return false;
  }

  if (!payload || String(payload.order_id || "") !== String(order_id || "")) return false;

  const exp = Number(payload.exp || 0);
  if (!Number.isFinite(exp) || Date.now() > exp) return false;

  return true;
}

// =====================================================
// Utilities: HMAC + Base64URL
// =====================================================
async function hmacHex(secret: string, message: string) {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const sig = await crypto.subtle.sign("HMAC", key, enc.encode(message));
  return [...new Uint8Array(sig)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

function timingSafeEqualHex(a: string, b: string) {
  if (a.length !== b.length) return false;
  let out = 0;
  for (let i = 0; i < a.length; i++) out |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return out === 0;
}

function base64urlEncode(str: string) {
  const b64 = btoa(unescape(encodeURIComponent(str)));
  return b64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function base64urlDecode(b64url: string) {
  const b64 = b64url.replace(/-/g, "+").replace(/_/g, "/") + "===".slice((b64url.length + 3) % 4);
  return decodeURIComponent(escape(atob(b64)));
}

// =====================================================
// Escape helpers
// =====================================================
function escapeHtml(s: any) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function escapeAttr(s: any) {
  return escapeHtml(s);
}

// =====================================================
// Hash utility for session fingerprinting
// =====================================================
async function hashString(str: string): Promise<string> {
  const data = new TextEncoder().encode(str);
  const hash = await crypto.subtle.digest("SHA-256", data);
  return [...new Uint8Array(hash)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

// ===== END PART 3/3 =====
