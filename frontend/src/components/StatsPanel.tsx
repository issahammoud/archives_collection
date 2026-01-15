import { useQuery } from '@tanstack/react-query'
import {
  Box,
  Stack,
  Badge,
  Group,
  ActionIcon,
  Tooltip,
  Popover,
  Select,
  MultiSelect,
  Text,
} from '@mantine/core'
import { DatePickerInput } from '@mantine/dates'
import { IconTag, IconBook } from '@tabler/icons-react'
import { useStore } from '../store'
import { articlesApi } from '../api'
import ArticlesLineChart from './ArticlesLineChart'
import ArchivePieChart from './ArchivePieChart'
import dayjs from 'dayjs'

function StatsPanel() {
  const { filters, totalCount, setFilters, resetPagination } = useStore()

  const { data: groupByData } = useQuery({
    queryKey: ['groupBy', filters, filters.groupBy],
    queryFn: () => articlesApi.getGroupBy(filters, filters.groupBy),
    enabled: !!filters.dateStart && !!filters.dateEnd,
  })

  const { data: archiveCountsData } = useQuery({
    queryKey: ['archiveCounts', filters],
    queryFn: () => articlesApi.getArchiveCounts(filters),
    enabled: !!filters.dateStart && !!filters.dateEnd,
  })

  const { data: tagsData } = useQuery({
    queryKey: ['tags'],
    queryFn: articlesApi.getTags,
  })

  const { data: archivesData } = useQuery({
    queryKey: ['archives'],
    queryFn: articlesApi.getArchives,
  })

  const { data: dateRange } = useQuery({
    queryKey: ['dateRange'],
    queryFn: articlesApi.getDateRange,
  })

  const formatCount = (count: number): string => {
    if (count >= 1000000) return `${(count / 1000000).toFixed(1)}M`
    if (count >= 1000) return `${(count / 1000).toFixed(1)}K`
    return String(count)
  }

  return (
    <Box
      bg="white"
      p="md"
      h="100%"
      style={{ borderRadius: 'var(--mantine-radius-md)' }}
    >
      <Stack gap={24} justify="space-between">
        {/* Article Count and Filters Row - All 4 elements in one row */}
        <Group gap="xs" align="center" justify="space-around">
          <Tooltip label={`${totalCount.toLocaleString()} articles`} withArrow position="bottom">
            <Badge
              size="xl"
              variant="filled"
              color="inky-red.5"
              radius="xl"
              px="xl"
              styles={{
                root: {
                  textTransform: 'none',
                  fontSize: '0.9rem',
                  // cursor: 'default',
                },
              }}
            >
              {formatCount(totalCount)}
            </Badge>
          </Tooltip>

          {/* Topic Filter */}
          <Popover position="bottom" withArrow shadow="md" width={220}>
            <Popover.Target>
              <Tooltip label="Filter by topic" withArrow position="bottom">
                <ActionIcon
                  variant={filters.tag ? 'filled' : 'light'}
                  color="inky-navy"
                  size="md"
                  radius="md"
                >
                  <IconTag size={16} />
                </ActionIcon>
              </Tooltip>
            </Popover.Target>
            <Popover.Dropdown>
              <Select
                placeholder="Search by topic"
                value={filters.tag}
                onChange={(value) => {
                  setFilters({ tag: value })
                  resetPagination()
                }}
                data={tagsData?.tags.map((tag) => ({ value: tag, label: tag })) || []}
                clearable
                searchable
                variant="default"
                radius="md"
                size="xs"
                comboboxProps={{ transitionProps: { transition: 'pop', duration: 200 } }}
              />
            </Popover.Dropdown>
          </Popover>

          {/* Archive Filter */}
          <Popover position="bottom" withArrow shadow="md" width={220}>
            <Popover.Target>
              <Tooltip label="Filter by archive" withArrow position="bottom">
                <ActionIcon
                  variant={filters.archives.length > 0 ? 'filled' : 'light'}
                  color="inky-navy"
                  size="md"
                  radius="md"
                >
                  <IconBook size={16} />
                </ActionIcon>
              </Tooltip>
            </Popover.Target>
            <Popover.Dropdown>
              <MultiSelect
                placeholder="Search by archive"
                value={filters.archives}
                onChange={(value) => {
                  setFilters({ archives: value })
                  resetPagination()
                }}
                data={archivesData?.archives || []}
                clearable
                searchable
                hidePickedOptions
                variant="default"
                radius="md"
                size="xs"
                comboboxProps={{ transitionProps: { transition: 'pop', duration: 200 } }}
              />
            </Popover.Dropdown>
          </Popover>

          <DatePickerInput
            type="range"
            placeholder="Select date range"
            value={[
              filters.dateStart ? dayjs(filters.dateStart).toDate() : null,
              filters.dateEnd ? dayjs(filters.dateEnd).toDate() : null,
            ]}
            onChange={(value) => {
              setFilters({
                dateStart: value[0] ? dayjs(value[0]).format('YYYY-MM-DD') : null,
                dateEnd: value[1] ? dayjs(value[1]).format('YYYY-MM-DD') : null,
              })
              resetPagination()
            }}
            minDate={dateRange?.min_date ? dayjs(dateRange.min_date).toDate() : undefined}
            maxDate={dateRange?.max_date ? dayjs(dateRange.max_date).toDate() : undefined}
            valueFormat="DD/MM/YYYY"
            variant="filled"
            radius="md"
            size="xs"
            clearable={false}
          />
        </Group>

        {/* Line Chart */}
        {groupByData && (
          <ArticlesLineChart data={groupByData.data} reversed={!filters.descOrder} />
        )}

        {/* Pie Chart */}
        {archiveCountsData && (
          <ArchivePieChart data={archiveCountsData.data} />
        )}
      </Stack>
    </Box>
  )
}

export default StatsPanel
