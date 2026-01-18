import { useMemo } from 'react'
import { LineChart } from '@mantine/charts'
import { Box } from '@mantine/core'
import dayjs from 'dayjs'
import type { GroupByData } from '../types'

interface ArticlesLineChartProps {
  data: GroupByData[]
  reversed?: boolean
}

function ArticlesLineChart({ data, reversed = false }: ArticlesLineChartProps) {
  // Early return if data is not a valid array
  if (!data || !Array.isArray(data) || data.length === 0) {
    return null
  }

  const chartData = useMemo(() => {
    // Defensive check inside useMemo as well
    if (!Array.isArray(data)) return []

    const sorted = [...data].sort((a, b) => {
      const dateA = new Date(a.date).getTime()
      const dateB = new Date(b.date).getTime()
      return reversed ? dateB - dateA : dateA - dateB
    })

    return sorted.map((item) => ({
      date: item.date ? dayjs(item.date).format('MMM YY') : '',
      count: typeof item.count === 'number' ? item.count : 0,
    }))
  }, [data, reversed])

  if (chartData.length === 0) {
    return null
  }

  return (
    <Box>
      <LineChart
        h={120}
        data={chartData}
        dataKey="date"
        series={[
          { name: 'count', color: 'inky-red.4', label: 'Articles' },
        ]}
        curveType="monotone"
        dotProps={{ r: 1, fill: 'var(--mantine-color-inky-red-4)', strokeWidth: 0 }}
        activeDotProps={{
          r: 3,
          fill: 'var(--mantine-color-inky-navy-5)',
          stroke: 'white',
          strokeWidth: 2,
        }}
        withYAxis={false}
        xAxisProps={{
          tickMargin: 0,
          style: { fontSize: 10 },
          padding: { left: 10, right: 10 }
        }}
        gridProps={{
          strokeDasharray: '3 3',
        }}
        withTooltip
        tooltipAnimationDuration={150}
        tooltipProps={{
          offset: 20,
          wrapperStyle: { zIndex: 100 },
          content: ({ payload, coordinate }) => {
            if (!payload || payload.length === 0) return null
            const item = payload[0]
            if (!item) return null
            const dateValue = typeof item.payload?.date === 'string' ? item.payload.date : String(item.payload?.date || '')
            const countValue = typeof item.value === 'number' ? item.value : 0
            return (
              <Box
                p="xs"
                style={{
                  backgroundColor: 'var(--mantine-color-dark-7)',
                  borderRadius: 'var(--mantine-radius-sm)',
                  fontSize: '12px',
                  color: 'white',
                  transform: 'translateY(-100%)',
                  marginTop: '-15px',
                  boxShadow: '0 2px 8px rgba(0,0,0,0.3)',
                }}
              >
                <div style={{ fontWeight: 500 }}>{dateValue}</div>
                <div style={{ color: 'var(--mantine-color-inky-red-4)' }}>
                  {countValue.toLocaleString()} articles
                </div>
              </Box>
            )
          },
        }}
        valueFormatter={(value) => typeof value === 'number' ? value.toLocaleString() : String(value)}
      />
    </Box>
  )
}

export default ArticlesLineChart
