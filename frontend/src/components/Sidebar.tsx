import { useState, useCallback, useEffect } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { notifications } from '@mantine/notifications'
import {
  Stack,
  Group,
  Badge,
  ActionIcon,
  Select,
  TextInput,
  MultiSelect,
  Text,
  Popover,
  Radio,
  Tooltip,
} from '@mantine/core'
import { DatePickerInput } from '@mantine/dates'
import {
  IconPlayerPlay,
  IconPlayerStop,
  IconArrowsSort,
  IconPhotoOff,
  IconChartBar,
  IconSearch,
  IconBook,
  IconCalendar,
  IconTag,
  IconDownload,
} from '@tabler/icons-react'
import { useStore } from '../store'
import { articlesApi, tasksApi } from '../api'
import dayjs from 'dayjs'

const ACCENT_COLOR = '#ff5757'
const ICON_COLOR = '#0097b2'

function Sidebar() {
  const {
    filters,
    setFilters,
    totalCount,
    collectionTaskId,
    setCollectionTaskId,
    isCollecting,
    setIsCollecting,
    resetPagination,
  } = useStore()

  const [searchQuery, setSearchQuery] = useState('')

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

  const startCollectionMutation = useMutation({
    mutationFn: () =>
      tasksApi.startCollection(
        filters.archives.length > 0 ? filters.archives : null,
        filters.dateStart || '',
        filters.dateEnd || ''
      ),
    onSuccess: (data) => {
      setCollectionTaskId(data.task_id)
      setIsCollecting(true)
      notifications.show({
        title: 'Collection Started',
        message: 'The data collection started. The interface will be updated each 5 seconds.',
        color: ACCENT_COLOR,
      })
    },
  })

  const stopCollectionMutation = useMutation({
    mutationFn: () => tasksApi.stopCollection(collectionTaskId || ''),
    onSuccess: () => {
      setCollectionTaskId(null)
      setIsCollecting(false)
      notifications.show({
        title: 'Collection Stopped',
        message: 'Collection stopped',
        color: ACCENT_COLOR,
      })
    },
  })

  useEffect(() => {
    if (!isCollecting || !collectionTaskId) return

    const interval = setInterval(async () => {
      try {
        const status = await tasksApi.getCollectionStatus(collectionTaskId)
        if (['SUCCESS', 'FAILURE', 'REVOKED'].includes(status.status)) {
          setIsCollecting(false)
          setCollectionTaskId(null)
        }
      } catch {
        // Ignore polling errors
      }
    }, 5000)

    return () => clearInterval(interval)
  }, [isCollecting, collectionTaskId, setIsCollecting, setCollectionTaskId])

  const handleSearch = useCallback(
    (e: React.KeyboardEvent<HTMLInputElement>) => {
      if (e.key === 'Enter') {
        setFilters({ query: searchQuery || null })
        resetPagination()
      }
    },
    [searchQuery, setFilters, resetPagination]
  )

  const formatCount = (count: number): string => {
    if (count >= 1000000) return `${(count / 1000000).toFixed(1)}M`
    if (count >= 1000) return `${(count / 1000).toFixed(1)}K`
    return String(count)
  }

  return (
    <Stack gap="sm">
      {/* Control buttons */}
      <Group justify="space-between">
        <Tooltip label={`${totalCount} articles`} withArrow position="top">
          <Badge
            size="xl"
            variant="light"
            color={ACCENT_COLOR}
            radius="xl"
            circle
            p={2}
          >
            <Text fw={300} size="xs">{formatCount(totalCount)}</Text>
          </Badge>
        </Tooltip>
        <Tooltip label="Start Collection" withArrow position="top">
          <ActionIcon
            variant="subtle"
            color={ACCENT_COLOR}
            onClick={() => startCollectionMutation.mutate()}
            disabled={isCollecting}
          >
            <IconPlayerPlay size={20} />
          </ActionIcon>
        </Tooltip>
        <Tooltip label="Stop Collection" withArrow position="top">
          <ActionIcon
            variant="subtle"
            color={ACCENT_COLOR}
            onClick={() => stopCollectionMutation.mutate()}
            disabled={!isCollecting}
          >
            <IconPlayerStop size={20} />
          </ActionIcon>
        </Tooltip>
      </Group>

      {/* Switches */}
      <Group justify="space-between">
        <Tooltip label="Flip order" withArrow position="top">
          <ActionIcon
            variant="subtle"
            color={ACCENT_COLOR}
            onClick={() => {
              setFilters({ descOrder: !filters.descOrder })
              resetPagination()
            }}
          >
            <IconArrowsSort size={20} />
          </ActionIcon>
        </Tooltip>
        <Tooltip label="Filter images" withArrow position="top">
          <ActionIcon
            variant={filters.hasImage ? 'filled' : 'subtle'}
            color={ACCENT_COLOR}
            onClick={() => {
              setFilters({ hasImage: !filters.hasImage })
              resetPagination()
            }}
          >
            <IconPhotoOff size={20} />
          </ActionIcon>
        </Tooltip>
        <Popover position="bottom" withArrow shadow="md">
          <Popover.Target>
            <Tooltip label="Group by" withArrow position="top">
              <ActionIcon variant="subtle" color={ACCENT_COLOR}>
                <IconChartBar size={20} />
              </ActionIcon>
            </Tooltip>
          </Popover.Target>
          <Popover.Dropdown>
            <Radio.Group
              value={filters.groupBy}
              onChange={(value) => setFilters({ groupBy: value as 'day' | 'month' | 'year' })}
              size="xs"
            >
              <Stack gap="xs">
                <Radio value="day" label="day" />
                <Radio value="month" label="month" />
                <Radio value="year" label="year" />
              </Stack>
            </Radio.Group>
          </Popover.Dropdown>
        </Popover>
      </Group>

      {/* Date picker */}
      <DatePickerInput
        type="range"
        label={<Text c="dimmed" fw={300}>Date</Text>}
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
        leftSection={<IconCalendar size={16} color={ICON_COLOR} />}
        variant="unstyled"
        clearable={false}
        styles={{ input: { borderBottom: '1px solid #eee' } }}
      />

      {/* Topic select */}
      <Select
        label={<Text c="dimmed" fw={300}>Topic</Text>}
        placeholder="Search by topic"
        value={filters.tag}
        onChange={(value) => {
          setFilters({ tag: value })
          resetPagination()
        }}
        data={tagsData?.tags.map((tag) => ({ value: tag, label: tag })) || []}
        clearable
        searchable
        leftSection={<IconTag size={16} color={ICON_COLOR} />}
        variant="unstyled"
        styles={{ input: { borderBottom: '1px solid #eee' } }}
        comboboxProps={{ transitionProps: { transition: 'pop', duration: 200 } }}
      />

      {/* Text search */}
      <TextInput
        label={<Text c="dimmed" fw={300}>Text</Text>}
        placeholder="Search by text"
        value={searchQuery}
        onChange={(e) => setSearchQuery(e.target.value)}
        onKeyDown={handleSearch}
        leftSection={<IconSearch size={16} color={ICON_COLOR} />}
        variant="unstyled"
        styles={{ input: { borderBottom: '1px solid #eee' } }}
      />

      {/* Archives multi-select */}
      <MultiSelect
        label={<Text c="dimmed" fw={300}>Archive</Text>}
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
        leftSection={<IconBook size={16} color={ICON_COLOR} />}
        variant="unstyled"
        styles={{ input: { borderBottom: '1px solid #eee' } }}
        comboboxProps={{ transitionProps: { transition: 'pop', duration: 200 } }}
      />
    </Stack>
  )
}

export default Sidebar
