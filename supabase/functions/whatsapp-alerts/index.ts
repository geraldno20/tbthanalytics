import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const WA_TOKEN = Deno.env.get("WHATSAPP_TOKEN")!;
const WA_PHONE_ID = Deno.env.get("WHATSAPP_PHONE_ID")!;
const RECIPIENTS = (Deno.env.get("WHATSAPP_RECIPIENTS") || "").split(",").filter(Boolean);

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Send a WhatsApp template message
async function sendWhatsApp(to: string, templateName: string, bodyParams: string[]) {
  const res = await fetch(`https://graph.facebook.com/v21.0/${WA_PHONE_ID}/messages`, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${WA_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      messaging_product: "whatsapp",
      to,
      type: "template",
      template: {
        name: templateName,
        language: { code: "en" },
        components: bodyParams.length ? [{
          type: "body",
          parameters: bodyParams.map(text => ({ type: "text", text })),
        }] : [],
      },
    }),
  });
  const data = await res.json();
  if (!res.ok) console.error("WhatsApp send error:", JSON.stringify(data));
  return data;
}

// Build alert messages
async function buildAlerts(): Promise<string[]> {
  const today = new Date().toISOString().slice(0, 10);
  const tomorrow = new Date(Date.now() + 86400000).toISOString().slice(0, 10);
  const nextWeek = new Date(Date.now() + 7 * 86400000).toISOString().slice(0, 10);

  const alerts: string[] = [];

  // 1. Overdue tasks (deadline passed, not complete)
  const { data: overdue } = await supabase
    .from("project_tasks")
    .select("task_key, deadline, projects(name)")
    .lt("deadline", today)
    .neq("status", "complete");

  if (overdue && overdue.length > 0) {
    const lines = overdue.slice(0, 10).map((t: any) =>
      `${t.projects?.name} - ${t.task_key} (due ${t.deadline})`
    );
    alerts.push(`OVERDUE (${overdue.length}): ${lines.join(", ")}`);
  }

  // 2. Air dates coming up in the next 7 days
  const { data: upcoming } = await supabase
    .from("projects")
    .select("name, season, episode, air_date")
    .gte("air_date", today)
    .lte("air_date", nextWeek)
    .order("air_date");

  if (upcoming && upcoming.length > 0) {
    const lines = upcoming.map((p: any) =>
      `S${p.season}E${p.episode || "?"} ${p.name} (${p.air_date})`
    );
    alerts.push(`AIRING THIS WEEK (${upcoming.length}): ${lines.join(", ")}`);
  }

  // 3. Pipeline summary
  const { data: projects } = await supabase.from("projects").select("id, air_date");
  const { data: tasks } = await supabase.from("project_tasks").select("project_id, task_key, status");

  if (projects && tasks) {
    const counts = { ideation: 0, preProd: 0, production: 0, prodComplete: 0, aired: 0 };
    for (const p of projects) {
      if (p.air_date && p.air_date <= today) { counts.aired++; continue; }
      const pTasks = tasks.filter((t: any) => t.project_id === p.id);
      const isComplete = (key: string) => pTasks.find((t: any) => t.task_key === key)?.status === "complete";
      if (isComplete("publish")) counts.prodComplete++;
      else if (isComplete("filming") || pTasks.find((t: any) => t.task_key === "filming")?.status === "in_progress") counts.production++;
      else if (isComplete("acceptance")) counts.preProd++;
      else counts.ideation++;
    }
    alerts.push(
      `PIPELINE: Ideation ${counts.ideation}, Planned ${counts.preProd}, Production ${counts.production + counts.prodComplete} (${counts.production} active, ${counts.prodComplete} done), Aired ${counts.aired}`
    );
  }

  return alerts;
}

Deno.serve(async (req) => {
  try {
    const alerts = await buildAlerts();

    if (alerts.length === 0) {
      return new Response(JSON.stringify({ message: "No alerts to send" }), { status: 200 });
    }

    const fullMessage = `TBTH Daily Update ${new Date().toLocaleDateString("en-HK")} | ${alerts.join(" | ")}`;
    // Replace newlines with " | " for template compliance
    const cleanMessage = fullMessage.replace(/\n/g, " | ").replace(/\s{4,}/g, " ");

    // Send to all recipients
    const results = [];
    for (const recipient of RECIPIENTS) {
      const result = await sendWhatsApp(recipient, "tbth_updates", [cleanMessage]);
      results.push({ to: recipient, result });
    }

    return new Response(JSON.stringify({ sent: results.length, results }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  } catch (error) {
    console.error("Error:", error);
    return new Response(JSON.stringify({ error: error.message }), { status: 500 });
  }
});
