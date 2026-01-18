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
  // Early return if data is not a valid array
  if (!data || !Array.isArray(data) || data.length === 0) {
    return null
  }

  const chartData = useMemo(() => {
    // Defensive check inside useMemo as well
    if (!Array.isArray(data) || data.length === 0) return []

    const total = data.reduce((sum, item) => sum + (typeof item.count === 'number' ? item.count : 0), 0)

    // Separate items >= 1% and < 1%
    const significantItems: { name: string; value: number; color: string }[] = []
    let otherValue = 0

    // Avoid division by zero
    if (total === 0) return []

    data.forEach((item, index) => {
      const count = typeof item.count === 'number' ? item.count : 0
      const percentage = (count / total) * 100
      if (percentage >= 1) {
        const archiveName = typeof item.archive === 'string' ? item.archive : String(item.archive || '')
        significantItems.push({
          name: archiveName.charAt(0).toUpperCase() + archiveName.slice(1),
          value: count,
          color: COLORS[index % COLORS.length],
        })
      } else {
        otherValue += count
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
    if (total === 0) return []
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
          if (item && total > 0) {
            const percentage = ((value / total) * 100).toFixed(0)
            return `${item.name} ${percentage}%`
          }
          return typeof value === 'number' ? value.toLocaleString() : String(value)
        }}
        withTooltip
        tooltipDataSource="segment"
        tooltipAnimationDuration={150}
        tooltipProps={{
          content: ({ payload }) => {
            if (!payload || payload.length === 0) return null
            const item = payload[0]?.payload
            if (!item) return null
            const percentage = total > 0 ? ((item.value / total) * 100).toFixed(1) : '0'
            const itemName = typeof item.name === 'string' ? item.name : String(item.name || '')
            const itemValue = typeof item.value === 'number' ? item.value : 0
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
                  {itemName}
                </Text>
                <Text size="xs" c="white">
                  {itemValue.toLocaleString()} articles ({percentage}%)
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
