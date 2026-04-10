import { useState, useEffect } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
)

// --- API response types ---

interface ScoreBucket {
  bucket: string
  count: number
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface TaskPassRate {
  task: string
  avg_score: number
  attempts: number
}

interface LabOption {
  value: string
  label: string
}

// --- Fetch helpers ---

async function fetchJson<T>(url: string, token: string): Promise<T> {
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${token}` },
  })
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`)
  }
  return (await res.json()) as T
}

const LABS: LabOption[] = [
  { value: 'lab-01', label: 'Lab 01' },
  { value: 'lab-02', label: 'Lab 02' },
  { value: 'lab-03', label: 'Lab 03' },
  { value: 'lab-04', label: 'Lab 04' },
]

// --- Component ---

export default function Dashboard() {
  const [selectedLab, setSelectedLab] = useState('lab-04')
  const [scores, setScores] = useState<ScoreBucket[]>([])
  const [timeline, setTimeline] = useState<TimelineEntry[]>([])
  const [passRates, setPassRates] = useState<TaskPassRate[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const token = localStorage.getItem('api_key') ?? ''

  useEffect(() => {
    if (!token) return

    const controller = new AbortController()
    setLoading(true)
    setError(null)

    const base = `/analytics`
    const params = `?lab=${selectedLab}`

    Promise.all([
      fetchJson<ScoreBucket[]>(`${base}/scores${params}`, token),
      fetchJson<TimelineEntry[]>(`${base}/timeline${params}`, token),
      fetchJson<TaskPassRate[]>(`${base}/pass-rates${params}`, token),
    ])
      .then(([s, t, p]) => {
        if (!controller.signal.aborted) {
          setScores(s)
          setTimeline(t)
          setPassRates(p)
          setLoading(false)
        }
      })
      .catch((err: Error) => {
        if (!controller.signal.aborted) {
          setError(err.message)
          setLoading(false)
        }
      })

    return () => controller.abort()
  }, [selectedLab, token])

  if (!token) {
    return <p>Please enter your API key to view the dashboard.</p>
  }

  if (loading) {
    return <p>Loading dashboard…</p>
  }

  if (error) {
    return <p>Error: {error}</p>
  }

  // --- Chart data ---

  const barData = {
    labels: scores.map((s) => s.bucket),
    datasets: [
      {
        label: 'Students',
        data: scores.map((s) => s.count),
        backgroundColor: 'rgba(54, 162, 235, 0.6)',
        borderColor: 'rgba(54, 162, 235, 1)',
        borderWidth: 1,
      },
    ],
  }

  const lineData = {
    labels: timeline.map((t) => t.date),
    datasets: [
      {
        label: 'Submissions per day',
        data: timeline.map((t) => t.submissions),
        borderColor: 'rgba(75, 192, 192, 1)',
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        fill: true,
        tension: 0.3,
      },
    ],
  }

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h1>Analytics Dashboard</h1>
        <label htmlFor="lab-select">Lab: </label>
        <select
          id="lab-select"
          value={selectedLab}
          onChange={(e) => setSelectedLab(e.target.value)}
        >
          {LABS.map((lab) => (
            <option key={lab.value} value={lab.value}>
              {lab.label}
            </option>
          ))}
        </select>
      </header>

      <section className="chart-row">
        <div className="chart-card">
          <h2>Score Distribution</h2>
          <Bar data={barData} options={{ responsive: true }} />
        </div>

        <div className="chart-card">
          <h2>Submissions Timeline</h2>
          <Line data={lineData} options={{ responsive: true }} />
        </div>
      </section>

      <section className="table-card">
        <h2>Pass Rates per Task</h2>
        {passRates.length === 0 ? (
          <p>No data available.</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Task</th>
                <th>Avg Score</th>
                <th>Attempts</th>
              </tr>
            </thead>
            <tbody>
              {passRates.map((row) => (
                <tr key={row.task}>
                  <td>{row.task}</td>
                  <td>{row.avg_score}</td>
                  <td>{row.attempts}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  )
}
