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
        withDots={false}
        withYAxis={false}
        xAxisProps={{
          tickMargin: 0,
          style: { fontSize: 10 },
          padding: { left: 10, right: 10 }
        }}
        gridProps={{
          strokeDasharray: '3 3',
        }}
        withTooltip={false}
        valueFormatter={(value) => value.toLocaleString()}
      />
    </Box>
  )
}

export default ArticlesLineChart
