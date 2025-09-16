// Define the base API URL
// It uses NEXT_PUBLIC_API_URL from environment variables if available,
// otherwise falls back to the local FastAPI backend at 127.0.0.1:8000
const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://127.0.0.1:8000";

// Fetch sensor alerts from the backend
// Supports pagination (skip & limit) and optional filtering by sensor_id
export async function fetchAlerts(skip = 0, limit = 20, sensor_id = "") {
  let url = `${API_BASE}/sensor_alerts/?skip=${skip}&limit=${limit}`;
  if (sensor_id) url += `&sensor_id=${sensor_id}`; // Add sensor_id filter if provided
  
  const res = await fetch(url);

  // If the response is not OK → throw error with details
  if (!res.ok) {
    const text = await res.text().catch(()=>"");
    throw new Error(`API error: ${res.status} ${text}`);
  }

  // Return JSON data from API
  return res.json();
}

// Fetch all sensor activity records from the backend
export async function fetchActivity() {
  const res = await fetch(`${API_BASE}/sensor_activity/`);

  // Handle error case
  if (!res.ok) throw new Error("Failed to fetch sensor activity");

  return res.json();
}

// Delete a specific alert by its ID
export async function deleteAlert(id) {
  const res = await fetch(`${API_BASE}/sensor_alerts/${id}`, {
    method: "DELETE", // Use HTTP DELETE method
  });

  // Handle API error
  if (!res.ok) {
    const text = await res.text().catch(()=>"");
    throw new Error(`Delete failed: ${res.status} ${text}`);
  }

  return res.json();
}
