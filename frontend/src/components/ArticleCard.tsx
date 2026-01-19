import { useState } from 'react'
import {
  Card,
  Text,
  Group,
  Stack,
  Box,
  ScrollArea,
  Badge,
  Tooltip,
  Blockquote,
  Skeleton,
} from '@mantine/core'
import { IconQuoteFilled } from '@tabler/icons-react'
import { LazyLoadImage } from 'react-lazy-load-image-component'
import 'react-lazy-load-image-component/src/effects/blur.css'
import dayjs from 'dayjs'
import type { Article } from '../types'

interface ArticleCardProps {
  article: Article
}

// Helper to safely convert value to string
const safeString = (value: unknown): string | null => {
  if (value === null || value === undefined) return null
  if (typeof value === 'string') return value
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

function ArticleCard({ article }: ArticleCardProps) {
  const [imageError, setImageError] = useState(false)
  const [imageLoaded, setImageLoaded] = useState(false)

  // Safely extract string values to prevent rendering objects
  const title = safeString(article.title)
  const content = safeString(article.content)
  const archive = safeString(article.archive)
  const tagValue = safeString(article.tag)

  const formattedDate = article.date
    ? dayjs(article.date).format('MMMM, DD YYYY')
    : ''

  const archiveName = archive
    ? archive.charAt(0).toUpperCase() + archive.slice(1)
    : ''

  const tag = tagValue
    ? tagValue.trim().charAt(0).toUpperCase() +
      tagValue.trim().slice(1).toLowerCase()
    : 'None'

  // Determine URL: Use fallback if error occurred or url is missing
  const imageUrl =
    article.image_url && !imageError
      ? article.image_url
      : 'https://placehold.co/600x400?text=Placeholder'

  return (
    <Card
      component="a"
      href={article.link}
      target="_blank"
      rel="noopener noreferrer"
      shadow="sm"
      // padding="lg"
      radius="md"
      withBorder
      h="100%"
      style={{
        textDecoration: 'none',
        color: 'inherit',
        display: 'flex',
        flexDirection: 'column',
      }}
      styles={{
        root: {
          '&:hover': {
            boxShadow: 'var(--mantine-shadow-lg)',
          },
        },
      }}
    >
      <Card.Section>
        <div style={{ height: 180, width: '100%', overflow: 'hidden', position: 'relative' }}>
          {!imageLoaded && (
            <Skeleton height={180} radius={0} style={{ position: 'absolute', top: 0, left: 0, right: 0 }} />
          )}
          <LazyLoadImage
            src={imageUrl}
            alt={title || 'Article image'}
            height={180}
            width="100%"
            effect="blur"
            onError={() => setImageError(true)}
            afterLoad={() => setImageLoaded(true)}
            style={{
              objectFit: 'cover',
              width: '100%',
              height: '100%',
              opacity: imageLoaded ? 1 : 0,
            }}
            wrapperProps={{
              style: { width: '100%', height: '100%', display: 'block' }
            }}
          />
        </div>
      </Card.Section>

      <Stack gap="xs" mt="md" style={{ flex: 1 }}>
        <Group justify="space-between">
          <Text size="xs" c="dimmed">{archiveName}</Text>
          <Text size="xs" c="dimmed">{formattedDate}</Text>
        </Group>

        <Tooltip label={title || 'Untitled'} withArrow position="top" mb="1px">
          <Text fw={600} size="md" lineClamp={1} c="inky-navy.6" title={title || ''}>
            {title || 'Untitled'}
          </Text>
        </Tooltip>

        <Blockquote color="inky-navy.5" radius="md" iconSize={35} icon={
            <Box style={{ transform: "rotate(180deg)" }}>
              <IconQuoteFilled
                size={20}
                color="var(--mantine-color-inky-red-5)"
              />
            </Box>
          }
          p="sm"
        >
        <ScrollArea h={90} offsetScrollbars scrollbarSize={6}>
          <Text size="sm" c="inky-dark" lh={1.6} ta="justify">
            {content || "No content available"}
          </Text>
        </ScrollArea>
      </Blockquote>

        <Group>
          <Badge color="inky-red.5" variant="subtle">
            {tag}
          </Badge>
        </Group>
      </Stack>
    </Card>
  )
}

export default ArticleCard