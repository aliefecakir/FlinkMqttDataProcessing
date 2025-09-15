
const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://127.0.0.1:8000";

export async function fetchAlerts(skip = 0, limit = 20, sensor_id = "") {
  let url = `${API_BASE}/sensor_alerts/?skip=${skip}&limit=${limit}`;
  if (sensor_id) url += `&sensor_id=${sensor_id}`;
  
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text().catch(()=>"");
    throw new Error(`API error: ${res.status} ${text}`);
  }
  return res.json();
}

export async function fetchActivity() {
  const res = await fetch(`${API_BASE}/sensor_activity/`);
  if (!res.ok) throw new Error("Sensor activity alınamadı");
  return res.json();
}

export async function deleteAlert(id) {
  const res = await fetch(`${API_BASE}/sensor_alerts/${id}`, {
    method: "DELETE",
  });
  if (!res.ok) {
    const text = await res.text().catch(()=>"");
    throw new Error(`Delete failed: ${res.status} ${text}`);
  }
  return res.json();
}
