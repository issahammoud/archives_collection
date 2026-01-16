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
  const chartData = useMemo(() => {
    const sorted = [...data].sort((a, b) => {
      const dateA = new Date(a.date).getTime()
      const dateB = new Date(b.date).getTime()
      return reversed ? dateB - dateA : dateA - dateB
    })

    return sorted.map((item) => ({
      date: item.date ? dayjs(item.date).format('MMM YY') : '',
      count: item.count,
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
                <div style={{ fontWeight: 500 }}>{item.payload?.date}</div>
                <div style={{ color: 'var(--mantine-color-inky-red-4)' }}>
                  {item.value?.toLocaleString()} articles
                </div>
              </Box>
            )
          },
        }}
        valueFormatter={(value) => value.toLocaleString()}
      />
    </Box>
  )
}

export default ArticlesLineChart
