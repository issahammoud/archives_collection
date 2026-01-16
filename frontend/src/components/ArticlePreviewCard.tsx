import { useState } from 'react'
import { Card, Image, Text, Box, Tooltip } from '@mantine/core'
import type { Article } from '../types'
import { imagesApi } from '../api'

interface ArticlePreviewCardProps {
  article: Article
  isSelected: boolean
  onClick: () => void
}

function ArticlePreviewCard({ article, isSelected, onClick }: ArticlePreviewCardProps) {
  const [imageError, setImageError] = useState(false)

  const imageUrl =
    article.image && !imageError
      ? imagesApi.getImageUrl(article.image)
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
        <Image
          src={imageUrl}
          alt={article.title || 'Article image'}
          h={80}
          fallbackSrc="https://placehold.co/600x400?text=Placeholder"
          onError={() => setImageError(true)}
        />
      </Card.Section>

      <Box mt="xs">
        <Tooltip label={article.title || 'Untitled'} withArrow position="top" mb="1px" multiline w={220}>
          <Text size="xs" fw={300} lineClamp={2} c="inky-navy.6" lh={1.3}>
            {article.title || 'Untitled'}
          </Text>
        </Tooltip>
      </Box>
    </Card>
  )
}

export default ArticlePreviewCard
