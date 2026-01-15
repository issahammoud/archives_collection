import { useMemo } from 'react'
import { PieChart } from '@mantine/charts'
import { Box } from '@mantine/core'
import type { ArchiveCountData } from '../types'

interface ArchivePieChartProps {
  data: ArchiveCountData[]
}

// Color palette for the pie chart
const COLORS = [
  'var(--mantine-color-inky-red-4)',
  'var(--mantine-color-inky-navy-5)',
  'var(--mantine-color-inky-navy-3)',
  '#4ECDC4',
  '#45B7D1',
  '#96CEB4',
  '#FFEAA7',
  '#DDA0DD',
  '#98D8C8',
  '#F7DC6F',
  '#BB8FCE',
  '#85C1E9',
]

function ArchivePieChart({ data }: ArchivePieChartProps) {
  const chartData = useMemo(() => {
    return data.map((item, index) => ({
      name: item.archive.charAt(0).toUpperCase() + item.archive.slice(1),
      value: item.count,
      color: COLORS[index % COLORS.length],
    }))
  }, [data])

  if (chartData.length === 0) {
    return null
  }

  return (
    <Box>
      <PieChart
        h={200}
        data={chartData}
        withLabels
        labelsType="percent"
        labelsPosition="outside"
        valueFormatter={(value) => value.toLocaleString()}
      />
    </Box>
  )
}

export default ArchivePieChart
