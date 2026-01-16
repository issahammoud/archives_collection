import { useMemo } from 'react'
import { PieChart } from '@mantine/charts'
import { Box, Text } from '@mantine/core'
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
    const total = data.reduce((sum, item) => sum + item.count, 0)

    // Separate items >= 1% and < 1%
    const significantItems: { name: string; value: number; color: string }[] = []
    let otherValue = 0

    data.forEach((item, index) => {
      const percentage = (item.count / total) * 100
      if (percentage >= 1) {
        significantItems.push({
          name: item.archive.charAt(0).toUpperCase() + item.archive.slice(1),
          value: item.count,
          color: COLORS[index % COLORS.length],
        })
      } else {
        otherValue += item.count
      }
    })

    // Add "Other" category if there are small items
    if (otherValue > 0) {
      significantItems.push({
        name: 'Other',
        value: otherValue,
        color: '#999999',
      })
    }

    return significantItems
  }, [data])

  if (chartData.length === 0) {
    return null
  }

  // Calculate total for percentage
  const total = useMemo(() => chartData.reduce((sum, item) => sum + item.value, 0), [chartData])

  // Create data with custom labels that include name and percentage
  const chartDataWithLabels = useMemo(() => {
    return chartData.map((item) => {
      const percentage = ((item.value / total) * 100).toFixed(0)
      return {
        ...item,
        label: `${item.name} ${percentage}%`,
      }
    })
  }, [chartData, total])

  return (
    <Box>
      <PieChart
        h={200}
        data={chartDataWithLabels}
        withLabels
        labelsPosition="outside"
        labelsType="value"
        valueFormatter={(value) => {
          // Find the item to get its custom label
          const item = chartDataWithLabels.find((d) => d.value === value)
          if (item) {
            const percentage = ((value / total) * 100).toFixed(0)
            return `${item.name} ${percentage}%`
          }
          return value.toLocaleString()
        }}
        withTooltip
        tooltipDataSource="segment"
        tooltipAnimationDuration={150}
        tooltipProps={{
          content: ({ payload }) => {
            if (!payload || payload.length === 0) return null
            const item = payload[0]?.payload
            if (!item) return null
            const percentage = ((item.value / total) * 100).toFixed(1)
            return (
              <Box
                p="xs"
                style={{
                  backgroundColor: 'var(--mantine-color-dark-7)',
                  borderRadius: 'var(--mantine-radius-sm)',
                  fontSize: '12px',
                  color: 'white',
                }}
              >
                <Text size="xs" fw={500} style={{ color: item.color }}>
                  {item.name}
                </Text>
                <Text size="xs" c="white">
                  {item.value.toLocaleString()} articles ({percentage}%)
                </Text>
              </Box>
            )
          },
        }}
      />
    </Box>
  )
}

export default ArchivePieChart
