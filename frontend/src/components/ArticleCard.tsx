import { useState } from 'react'
import {
  Card,
  Image,
  Text,
  Group,
  Stack,
  Box,
  ScrollArea,
} from '@mantine/core'
import { IconQuote } from '@tabler/icons-react'
import dayjs from 'dayjs'
import type { Article } from '../types'
import { imagesApi } from '../api'

interface ArticleCardProps {
  article: Article
}

function ArticleCard({ article }: ArticleCardProps) {
  const [imageError, setImageError] = useState(false)

  const formattedDate = article.date
    ? dayjs(article.date).format('MMMM, DD YYYY')
    : ''

  const archiveName = article.archive
    ? article.archive.charAt(0).toUpperCase() + article.archive.slice(1)
    : ''

  const tag = article.tag
    ? article.tag.trim().charAt(0).toUpperCase() +
      article.tag.trim().slice(1).toLowerCase()
    : 'None'

  const imageUrl =
    article.image && !imageError
      ? imagesApi.getImageUrl(article.image)
      : 'https://placehold.co/600x400?text=Placeholder'

  return (
    <Card
      component="a"
      href={article.link}
      target="_blank"
      rel="noopener noreferrer"
      shadow="sm"
      padding={0}
      radius="md"
      withBorder
      h="100%"
      style={{
        textDecoration: 'none',
        color: 'inherit',
        display: 'flex',
        flexDirection: 'column',
        transition: 'box-shadow 150ms ease',
        cursor: 'pointer',
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.15)'
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.boxShadow = ''
      }}
    >
      <Card.Section withBorder inheritPadding py="xs" px="md">
        <Group justify="space-between">
          <Text fw={600} size="sm">{archiveName}</Text>
          <Text size="xs" c="dimmed" fs="italic">
            {formattedDate}
          </Text>
        </Group>
      </Card.Section>

      <Card.Section>
        <Image
          src={imageUrl}
          alt={article.title || 'Article image'}
          h={180}
          fallbackSrc="https://placehold.co/600x400?text=Placeholder"
          onError={() => setImageError(true)}
        />
      </Card.Section>

      <Stack gap="sm" p="md" style={{ flex: 1 }}>
        <Text fw={500} size="sm" lineClamp={2} title={article.title || ''}>
          {article.title || 'Untitled'}
        </Text>

        <Box
          pl="sm"
          py="xs"
          pr="xs"
          pos="relative"
          style={{
            borderLeft: '3px solid #0097b2',
            backgroundColor: 'var(--mantine-color-gray-0)',
            borderRadius: '0 var(--mantine-radius-sm) var(--mantine-radius-sm) 0',
            flex: 1,
          }}
        >
          <IconQuote
            size={14}
            color="#ff5757"
            style={{
              position: 'absolute',
              top: 6,
              left: 6,
              transform: 'scaleX(-1) scaleY(-1)',
            }}
          />
          <ScrollArea h={100} offsetScrollbars scrollbarSize={6}>
            <Text size="xs" c="dimmed" ta="justify" lh={1.6} pl="md">
              {article.content || 'No content available'}
            </Text>
          </ScrollArea>
        </Box>

        <Text size="xs" c="dimmed" fs="italic" lineClamp={1}>
          {tag}
        </Text>
      </Stack>
    </Card>
  )
}

export default ArticleCard
