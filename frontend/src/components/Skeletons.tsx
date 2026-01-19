import {
  Card,
  Skeleton,
  Stack,
  Group,
  Box,
  SimpleGrid,
  Grid,
} from '@mantine/core'

// Skeleton for the small preview cards (matches ArticlePreviewCard)
export function ArticlePreviewCardSkeleton() {
  return (
    <Card shadow="xs" padding="xs" radius="md" withBorder>
      <Card.Section>
        <Skeleton height={80} radius={0} />
      </Card.Section>
      <Box mt="xs">
        <Skeleton height={12} radius="sm" />
        <Skeleton height={12} mt={6} radius="sm" width="70%" />
      </Box>
    </Card>
  )
}

// Skeleton for the large article card (matches ArticleCard)
export function ArticleCardSkeleton() {
  return (
    <Card shadow="sm" padding="lg" radius="md" withBorder h="100%">
      <Card.Section>
        <Skeleton height={180} radius={0} />
      </Card.Section>

      <Stack gap="xs" mt="md">
        {/* Archive and date row */}
        <Group justify="space-between">
          <Skeleton height={14} width={80} radius="sm" />
          <Skeleton height={14} width={100} radius="sm" />
        </Group>

        {/* Title */}
        <Skeleton height={20} radius="sm" />

        {/* Content block (blockquote area) */}
        <Box
          p="sm"
          style={{
            borderLeft: '3px solid var(--mantine-color-gray-3)',
            borderRadius: 'var(--mantine-radius-md)',
            backgroundColor: 'var(--mantine-color-gray-0)',
          }}
        >
          <Skeleton height={12} radius="sm" />
          <Skeleton height={12} mt={8} radius="sm" />
          <Skeleton height={12} mt={8} radius="sm" />
          <Skeleton height={12} mt={8} radius="sm" width="80%" />
        </Box>

        {/* Tag badge */}
        <Group>
          <Skeleton height={22} width={60} radius="xl" />
        </Group>
      </Stack>
    </Card>
  )
}

// Skeleton for the entire carousel (9 preview cards + 1 large card) - Desktop
export function CarouselSkeleton({ isMobile = false }: { isMobile?: boolean }) {
  // Mobile skeleton - single card with 20 dots
  if (isMobile) {
    return (
      <Box pos="relative" pb="xl">
        <Box
          bg="white"
          p="xs"
          style={{ borderRadius: 'var(--mantine-radius-md)' }}
        >
          <ArticleCardSkeleton />
        </Box>

        {/* Page indicator dots skeleton - 20 dots for mobile */}
        <Group
          justify="center"
          gap={4}
          wrap="nowrap"
          mt="md"
        >
          {Array.from({ length: 20 }).map((_, index) => (
            <Box
              key={index}
              style={{
                width: 8,
                height: 8,
                borderRadius: '50%',
                backgroundColor: 'var(--mantine-color-gray-3)',
                flexShrink: 0,
              }}
            />
          ))}
        </Group>
      </Box>
    )
  }

  // Desktop skeleton - 9 preview cards + 1 large card
  return (
    <Box pos="relative">
      <Grid gutter="xl" align="stretch">
        {/* Left side: 9 small preview card skeletons */}
        <Grid.Col span={5}>
          <SimpleGrid cols={3} spacing="sm">
            {Array.from({ length: 9 }).map((_, index) => (
              <ArticlePreviewCardSkeleton key={index} />
            ))}
          </SimpleGrid>
        </Grid.Col>

        {/* Right side: Large card skeleton */}
        <Grid.Col span={7}>
          <Box style={{ height: '100%' }}>
            <ArticleCardSkeleton />
          </Box>
        </Grid.Col>
      </Grid>

      {/* Page indicator dots skeleton */}
      <Group
        justify="center"
        gap={8}
        style={{
          position: 'absolute',
          bottom: -24,
          left: '50%',
          transform: 'translateX(-50%)',
        }}
      >
        {Array.from({ length: 5 }).map((_, index) => (
          <Box
            key={index}
            style={{
              width: 6,
              height: 6,
              borderRadius: '50%',
              backgroundColor: 'var(--mantine-color-gray-3)',
            }}
          />
        ))}
      </Group>
    </Box>
  )
}

// Skeleton for line chart
export function LineChartSkeleton() {
  return (
    <Box h={120}>
      <Skeleton height={120} radius="sm" />
    </Box>
  )
}

// Skeleton for pie chart
export function PieChartSkeleton() {
  return (
    <Box h={200} style={{ display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
      <Skeleton height={160} width={160} circle />
    </Box>
  )
}

// Skeleton for the stats panel header (badge + filters)
export function StatsPanelHeaderSkeleton() {
  return (
    <Group gap="xs" align="center" justify="space-around">
      {/* Badge skeleton */}
      <Skeleton height={32} width={70} radius="xl" />
      {/* Archive filter icon */}
      <Skeleton height={28} width={28} radius="md" />
      {/* Date picker */}
      <Skeleton height={28} width={200} radius="md" />
    </Group>
  )
}

// Full stats panel skeleton
export function StatsPanelSkeleton() {
  return (
    <Box
      bg="white"
      p="md"
      h="100%"
      style={{ borderRadius: 'var(--mantine-radius-md)' }}
    >
      <Stack gap={24} justify="space-between">
        <StatsPanelHeaderSkeleton />
        <LineChartSkeleton />
        <PieChartSkeleton />
      </Stack>
    </Box>
  )
}
