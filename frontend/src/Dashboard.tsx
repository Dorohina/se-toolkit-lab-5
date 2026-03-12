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

interface ScoreBucket {
  bucket: string
  count: number
}

interface ScoresResponse {
  lab_id: string
  lab_name: string
  buckets: ScoreBucket[]
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface TimelineResponse {
  lab_id: string
  timeline: TimelineEntry[]
}

interface TaskPassRate {
  task_id: string
  task_name: string
  pass_rate: number
  total_submissions: number
  passed_submissions: number
}

interface PassRatesResponse {
  lab_id: string
  tasks: TaskPassRate[]
}

interface Lab {
  id: string
  name: string
}

type FetchState<T> =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; data: T }
  | { status: 'error'; message: string }

interface DashboardProps {
  labs: Lab[]
  apiKey: string
}

function Dashboard({ labs, apiKey }: DashboardProps) {
  const [selectedLabId, setSelectedLabId] = useState<string>(
    () => labs[0]?.id ?? '',
  )

  const [scoresState, setScoresState] = useState<FetchState<ScoresResponse>>({
    status: 'idle',
  })
  const [timelineState, setTimelineState] = useState<
    FetchState<TimelineResponse>
  >({ status: 'idle' })
  const [passRatesState, setPassRatesState] = useState<
    FetchState<PassRatesResponse>
  >({ status: 'idle' })

  useEffect(() => {
    if (!selectedLabId) return

    const fetchWithAuth = async <T,>(
      url: string,
      setState: (state: FetchState<T>) => void,
    ) => {
      setState({ status: 'loading' })
      try {
        const res = await fetch(url, {
          headers: { Authorization: `Bearer ${apiKey}` },
        })
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data: T = await res.json()
        setState({ status: 'success', data })
      } catch (err) {
        setState({
          status: 'error',
          message: err instanceof Error ? err.message : 'Unknown error',
        })
      }
    }

    const labParam = `lab=${encodeURIComponent(selectedLabId)}`
    fetchWithAuth<ScoresResponse>(
      `/analytics/scores?${labParam}`,
      setScoresState,
    )
    fetchWithAuth<TimelineResponse>(
      `/analytics/timeline?${labParam}`,
      setTimelineState,
    )
    fetchWithAuth<PassRatesResponse>(
      `/analytics/pass-rates?${labParam}`,
      setPassRatesState,
    )
  }, [selectedLabId, apiKey])

  const scoresChartData =
    scoresState.status === 'success'
      ? {
          labels: scoresState.data.buckets.map((b) => b.bucket),
          datasets: [
            {
              label: 'Submissions',
              data: scoresState.data.buckets.map((b) => b.count),
              backgroundColor: 'rgba(54, 162, 235, 0.6)',
              borderColor: 'rgba(54, 162, 235, 1)',
              borderWidth: 1,
            },
          ],
        }
      : { labels: [] as string[], datasets: [] }

  const timelineChartData =
    timelineState.status === 'success'
      ? {
          labels: timelineState.data.timeline.map((t) => t.date),
          datasets: [
            {
              label: 'Submissions per Day',
              data: timelineState.data.timeline.map((t) => t.submissions),
              borderColor: 'rgba(75, 192, 192, 1)',
              backgroundColor: 'rgba(75, 192, 192, 0.2)',
              tension: 0.1,
            },
          ],
        }
      : { labels: [] as string[], datasets: [] }

  const chartOptions = {
    responsive: true,
    plugins: {
      legend: {
        position: 'top' as const,
      },
      title: {
        display: true,
        text: '',
      },
    },
  }

  return (
    <div className="dashboard">
      <div className="dashboard-header">
        <h2>Analytics Dashboard</h2>
        <select
          value={selectedLabId}
          onChange={(e) => setSelectedLabId(e.target.value)}
          className="lab-selector"
        >
          {labs.map((lab) => (
            <option key={lab.id} value={lab.id}>
              {lab.name}
            </option>
          ))}
        </select>
      </div>

      <div className="dashboard-content">
        <section className="chart-section">
          <h3>Score Distribution</h3>
          {scoresState.status === 'loading' && <p>Loading...</p>}
          {scoresState.status === 'error' && (
            <p className="error">Error: {scoresState.message}</p>
          )}
          {scoresState.status === 'success' && (
            <Bar data={scoresChartData} options={chartOptions} />
          )}
        </section>

        <section className="chart-section">
          <h3>Submissions Timeline</h3>
          {timelineState.status === 'loading' && <p>Loading...</p>}
          {timelineState.status === 'error' && (
            <p className="error">Error: {timelineState.message}</p>
          )}
          {timelineState.status === 'success' && (
            <Line data={timelineChartData} options={chartOptions} />
          )}
        </section>

        <section className="table-section">
          <h3>Pass Rates per Task</h3>
          {passRatesState.status === 'loading' && <p>Loading...</p>}
          {passRatesState.status === 'error' && (
            <p className="error">Error: {passRatesState.message}</p>
          )}
          {passRatesState.status === 'success' && (
            <table className="pass-rates-table">
              <thead>
                <tr>
                  <th>Task</th>
                  <th>Total</th>
                  <th>Passed</th>
                  <th>Pass Rate</th>
                </tr>
              </thead>
              <tbody>
                {passRatesState.data.tasks.map((task) => (
                  <tr key={task.task_id}>
                    <td>{task.task_name}</td>
                    <td>{task.total_submissions}</td>
                    <td>{task.passed_submissions}</td>
                    <td>{(task.pass_rate * 100).toFixed(1)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>
      </div>
    </div>
  )
}

export default Dashboard
