import { useState } from 'react'
import { Card, Image, Text, Box, Tooltip } from '@mantine/core'
import { LazyLoadImage } from 'react-lazy-load-image-component'
import 'react-lazy-load-image-component/src/effects/blur.css' // The blur effect styles
import type { Article } from '../types'

interface ArticlePreviewCardProps {
  article: Article
  isSelected: boolean
  onClick: () => void
}

function ArticlePreviewCard({ article, isSelected, onClick }: ArticlePreviewCardProps) {
  const [imageError, setImageError] = useState(false)

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
        <LazyLoadImage
            src={imageUrl}
            alt={article.title || 'Article image'}
            height={80}
            width="100%"
            effect="blur" // Adds the smooth fade-in
            onError={() => setImageError(true)}
            
            // Manual styling to match Mantine's 'fit="cover"'
            style={{ 
              objectFit: 'cover', 
              width: '100%', 
            }}
            
            // Ensures the library's wrapper span also fills the space
            wrapperProps={{
                style: { width: '100%', height: '100%', display: 'block' }
            }}
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
