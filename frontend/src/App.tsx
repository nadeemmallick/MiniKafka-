import { useState, useEffect } from 'react'
import './App.css'

function App() {
  const [topics, setTopics] = useState<any[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    // Fetch topics on mount
    fetchTopics()
  }, [])

  const fetchTopics = async () => {
    try {
      const response = await fetch('/api/topics')
      const data = await response.json()
      setTopics(data.topics || [])
    } catch (error) {
      console.error('Error fetching topics:', error)
    } finally {
      setLoading(false)
    }
  }

  const createTopic = async (name: string, partitionCount: number) => {
    try {
      const response = await fetch('/api/topics', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, partitionCount }),
      })
      const data = await response.json()
      if (response.ok) {
        fetchTopics()
      } else {
        alert(data.error || 'Failed to create topic')
      }
    } catch (error) {
      console.error('Error creating topic:', error)
      alert('Failed to create topic')
    }
  }

  return (
    <div className="min-h-screen bg-gray-900 text-white">
      <div className="container mx-auto px-4 py-8">
        <header className="mb-8">
          <h1 className="text-4xl font-bold mb-2">🚀 MiniKafka Dashboard</h1>
          <p className="text-gray-400">Interactive message broker visualization</p>
        </header>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Left Panel - Controls */}
          <div className="bg-gray-800 rounded-lg p-6">
            <h2 className="text-xl font-semibold mb-4">Create Topic</h2>
            <CreateTopicForm onCreateTopic={createTopic} />
          </div>

          {/* Center Panel - Topics */}
          <div className="bg-gray-800 rounded-lg p-6">
            <h2 className="text-xl font-semibold mb-4">Topics</h2>
            {loading ? (
              <p>Loading...</p>
            ) : topics.length === 0 ? (
              <p className="text-gray-400">No topics created yet</p>
            ) : (
              <ul className="space-y-2">
                {topics.map((topic) => (
                  <li key={topic.name} className="bg-gray-700 rounded p-3">
                    <div className="font-medium">{topic.name}</div>
                    <div className="text-sm text-gray-400">
                      {topic.partitionCount} partitions • {topic.messageCount} messages
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </div>

          {/* Right Panel - Statistics */}
          <div className="bg-gray-800 rounded-lg p-6">
            <h2 className="text-xl font-semibold mb-4">Statistics</h2>
            <StatisticsPanel />
          </div>
        </div>
      </div>
    </div>
  )
}

function CreateTopicForm({ onCreateTopic }: { onCreateTopic: (name: string, partitions: number) => void }) {
  const [name, setName] = useState('')
  const [partitions, setPartitions] = useState(3)

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (name.trim()) {
      onCreateTopic(name, partitions)
      setName('')
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <div>
        <label className="block text-sm font-medium mb-2">Topic Name</label>
        <input
          type="text"
          value={name}
          onChange={(e) => setName(e.target.value)}
          className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-white"
          placeholder="e.g., orders"
        />
      </div>
      <div>
        <label className="block text-sm font-medium mb-2">Partitions</label>
        <input
          type="number"
          value={partitions}
          onChange={(e) => setPartitions(parseInt(e.target.value))}
          min="1"
          max="10"
          className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-white"
        />
      </div>
      <button
        type="submit"
        className="w-full bg-blue-600 hover:bg-blue-700 text-white font-medium py-2 px-4 rounded"
      >
        Create Topic
      </button>
    </form>
  )
}

function StatisticsPanel() {
  const [stats, setStats] = useState<any>(null)

  useEffect(() => {
    const fetchStats = async () => {
      try {
        const response = await fetch('/api/statistics')
        const data = await response.json()
        setStats(data.statistics)
      } catch (error) {
        console.error('Error fetching statistics:', error)
      }
    }

    fetchStats()
    const interval = setInterval(fetchStats, 1000)
    return () => clearInterval(interval)
  }, [])

  if (!stats) return <p>Loading statistics...</p>

  return (
    <div className="space-y-4">
      <div className="bg-gray-700 rounded p-4">
        <div className="text-2xl font-bold">{stats.totalMessages}</div>
        <div className="text-sm text-gray-400">Total Messages</div>
      </div>
      <div className="bg-gray-700 rounded p-4">
        <div className="text-2xl font-bold">{stats.messagesPerSecond}</div>
        <div className="text-sm text-gray-400">Messages/Second</div>
      </div>
      <div className="bg-gray-700 rounded p-4">
        <div className="text-2xl font-bold">{stats.topicsCount}</div>
        <div className="text-sm text-gray-400">Topics</div>
      </div>
    </div>
  )
}

export default App
