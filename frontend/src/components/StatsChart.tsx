import { useMemo } from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import { Box } from '@mantine/core'
import dayjs from 'dayjs'
import type { GroupByData } from '../types'

interface StatsChartProps {
  data: GroupByData[]
  reversed?: boolean
}

function StatsChart({ data, reversed = false }: StatsChartProps) {
  const chartData = useMemo(() => {
    const sorted = [...data].sort((a, b) => {
      const dateA = new Date(a.date).getTime()
      const dateB = new Date(b.date).getTime()
      return reversed ? dateB - dateA : dateA - dateB
    })

    return sorted.map((item) => ({
      ...item,
      label: item.date ? dayjs(item.date).format('MMM DD, YYYY') : '',
    }))
  }, [data, reversed])

  if (chartData.length === 0) {
    return null
  }

  return (
    <Box h={80}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={chartData} margin={{ top: 0, right: 0, left: 0, bottom: 0 }}>
          <XAxis
            dataKey="label"
            tick={{ fontSize: 10 }}
            interval="preserveStartEnd"
            axisLine={false}
            tickLine={false}
          />
          <YAxis hide />
          <Tooltip
            formatter={(value: number) => [value, 'Articles']}
            labelFormatter={(label) => label}
            contentStyle={{
              borderRadius: 8,
              border: '1px solid #e9ecef',
              boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
            }}
          />
          <Bar
            dataKey="count"
            fill="#0097b2"
            radius={[2, 2, 0, 0]}
          />
        </BarChart>
      </ResponsiveContainer>
    </Box>
  )
}

export default StatsChart
