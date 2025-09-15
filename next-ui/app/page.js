"use client";
import { useEffect, useState } from "react";
import { fetchAlerts, deleteAlert, fetchActivity } from "../lib/api";

/* cd next-ui // npm run dev */

export default function Page() {
  const [alerts, setAlerts] = useState([]);
  const [activity, setActivity] = useState([]);
  const [latestActivity, setLatestActivity] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [deletingIds, setDeletingIds] = useState(new Set());
  const [activeTab, setActiveTab] = useState("alerts");

  useEffect(() => {
    let mounted = true;
    setLoading(true);
    setError(null);

    if (activeTab === "alerts") {
      fetchAlerts()
        .then(data => { if (mounted) setAlerts(data); })
        .catch(err => { if (mounted) setError(err.message); })
        .finally(() => { if (mounted) setLoading(false); });
    } else if (activeTab === "activity") {
      fetchActivity()
        .then(data => {
          if (!mounted) return;
          setActivity(data);
          const latestMap = new Map();
          data.forEach(a => {
            const current = latestMap.get(a.sensor_id);
            if (!current || new Date(a.activity_date) > new Date(current.activity_date)) {
              latestMap.set(a.sensor_id, a);
            }
          });
          setLatestActivity(Array.from(latestMap.values()));
        })
        .catch(err => { if (mounted) setError(err.message); })
        .finally(() => { if (mounted) setLoading(false); });
    }

    return () => { mounted = false; };
  }, [activeTab]);

  const handleDelete = async (id) => {
    if (!confirm("Are you sure you want to delete the alert?")) return;
    const prev = alerts;
    setAlerts(a => a.filter(x => x.id !== id));
    setDeletingIds(s => new Set(s).add(id));
    try {
      await deleteAlert(id);
      setDeletingIds(s => {
        const copy = new Set(s);
        copy.delete(id);
        return copy;
      });
    } catch (err) {
      setAlerts(prev);
      setDeletingIds(s => {
        const copy = new Set(s);
        copy.delete(id);
        return copy;
      });
      alert("Deleting process failed: " + err.message);
    }
  };

  if (loading) return <div className="p-6">Loading...</div>;
  if (error) return <div className="p-6 text-red-600">Error: {error}</div>;

  return (
    <div className="p-6 max-w-4xl mx-auto">
      <h1 className="text-2xl font-bold mb-4">Sensor Management</h1>

      <div className="flex gap-4 mb-4">
        <button
          className={`px-4 py-2 rounded ${activeTab === "alerts" ? "bg-blue-600 text-white" : "bg-gray-800"}`}
          onClick={() => setActiveTab("alerts")}
        >
          Sensor Alerts
        </button>
        <button
          className={`px-4 py-2 rounded ${activeTab === "activity" ? "bg-blue-600 text-white" : "bg-gray-800"}`}
          onClick={() => setActiveTab("activity")}
        >
          Sensor Activity
        </button>
      </div>

      {activeTab === "alerts" && (
        <ul>
          {alerts.map(alert => (
            <li
              key={alert.id}
              className="flex justify-between border-b border-gray-400 px-4 py-2 hover:bg-gray-800 hover:text-white hover:rounded-md transition-colors duration-350"
            >
              <div>
                <div className="font-semibold">{alert.id} | {alert.sensor_id} ({alert.alert_type})</div>
                <div className="text-sm text-gray-300">
                  Value: {alert.value}, Time: {new Date(alert.alert_time).toLocaleString()}
                </div>
              </div>
              <div>
                <button
                  onClick={() => handleDelete(alert.id)}
                  disabled={deletingIds.has(alert.id)}
                  className="px-3 py-1 rounded bg-red-700 text-white disabled:opacity-50"
                >
                  {deletingIds.has(alert.id) ? "Deleting..." : "Delete"}
                </button>
              </div>
            </li>
          ))}
          {alerts.length === 0 && <li className="px-4 py-2 text-gray-600 text-center">Alert not found.</li>}
        </ul>
      )}

      {activeTab === "activity" && (
        <ul>
          {latestActivity.map(a => (
            <li
              key={a.id}
              className="flex justify-between border-b border-gray-400 px-4 py-2 hover:bg-gray-800 hover:text-white hover:rounded-md transition-colors duration-150"
            >
              <span>{a.sensor_id}</span>
              <span>Last Active: {new Date(a.activity_date).toLocaleString()}</span>
            </li>
          ))}
          {latestActivity.length === 0 && (
            <li className="px-4 py-2 text-gray-600 text-center">No active sensors found.</li>
          )}
        </ul>
      )}
    </div>
  );
}
