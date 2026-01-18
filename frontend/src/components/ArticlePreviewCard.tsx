import { useState } from 'react'
import { Card, Text, Box, Tooltip, Skeleton } from '@mantine/core'
import { LazyLoadImage } from 'react-lazy-load-image-component'
import 'react-lazy-load-image-component/src/effects/blur.css'
import type { Article } from '../types'

interface ArticlePreviewCardProps {
  article: Article
  isSelected: boolean
  onClick: () => void
}

// Helper to safely convert value to string
const safeString = (value: unknown): string | null => {
  if (value === null || value === undefined) return null
  if (typeof value === 'string') return value
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

function ArticlePreviewCard({ article, isSelected, onClick }: ArticlePreviewCardProps) {
  const [imageError, setImageError] = useState(false)
  const [imageLoaded, setImageLoaded] = useState(false)

  // Safely extract string values to prevent rendering objects
  const title = safeString(article.title)

  const imageUrl =
    article.image_url && !imageError
      ? article.image_url
      : 'https://placehold.co/600x400?text=Placeholder'

  return (
    <Card
      shadow="xs"
      padding="xs"
      radius="md"
      withBorder
      onClick={onClick}
      style={{
        cursor: 'pointer',
        borderColor: isSelected ? 'var(--mantine-color-inky-red-4)' : undefined,
        borderWidth: isSelected ? 2 : 1,
      }}
      styles={{
        root: {
          '&:hover': {
            boxShadow: 'var(--mantine-shadow-md)',
          },
        },
      }}
    >
      <Card.Section>
        <div style={{ height: 80, width: '100%', position: 'relative' }}>
          {!imageLoaded && (
            <Skeleton height={80} radius={0} style={{ position: 'absolute', top: 0, left: 0, right: 0 }} />
          )}
          <LazyLoadImage
            src={imageUrl}
            alt={title || 'Article image'}
            height={80}
            width="100%"
            effect="blur"
            onError={() => setImageError(true)}
            afterLoad={() => setImageLoaded(true)}
            style={{
              objectFit: 'cover',
              width: '100%',
              opacity: imageLoaded ? 1 : 0,
            }}
            wrapperProps={{
              style: { width: '100%', height: '100%', display: 'block' }
            }}
          />
        </div>
      </Card.Section>

      <Box mt="xs">
        <Tooltip label={title || 'Untitled'} withArrow position="top" mb="1px" multiline w={220}>
          <Text size="xs" fw={300} lineClamp={2} c="inky-navy.6" lh={1.3}>
            {title || 'Untitled'}
          </Text>
        </Tooltip>
      </Box>
    </Card>
  )
}

export default ArticlePreviewCard
