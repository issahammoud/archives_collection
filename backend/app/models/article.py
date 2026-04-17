from sqlalchemy import (
    Column,
    BigInteger,
    String,
    Date,
    Text,
    Computed,
    Index,
    ForeignKey,
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import TSVECTOR
from pgvector.sqlalchemy import HALFVEC

from app.core.database import Base

EMBEDDING_DIM = 256


class Article(Base):
    __tablename__ = "articles"

    rowid = Column(BigInteger, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    archive = Column(String, nullable=False)
    image = Column(Text, nullable=True)
    title = Column(String, nullable=True)
    content = Column(String, nullable=True)
    tag = Column(String, nullable=True)
    link = Column(String, nullable=False)
    hash = Column(
        BigInteger,
        Computed("hashtext(link)::BIGINT", persisted=True),
        nullable=False,
        unique=True,
    )
    text_searchable = Column(TSVECTOR, nullable=True)

    # Relationship to embedding
    embedding_rel = relationship("ArticleEmbedding", back_populates="article", uselist=False)

    __table_args__ = (
        Index("articles_date_rowid_index", "date", "rowid"),
        Index(
            "articles_text_searchable_index",
            "text_searchable",
            postgresql_using="gin",
        ),
    )


class ArticleEmbedding(Base):
    __tablename__ = "article_embeddings"

    rowid = Column(BigInteger, ForeignKey("articles.rowid", ondelete="CASCADE"), primary_key=True)
    embedding = Column(HALFVEC(EMBEDDING_DIM), nullable=True)

    # Relationship back to article
    article = relationship("Article", back_populates="embedding_rel")

    def __repr__(self):
        return f"<Article(rowid={self.rowid}, title={self.title[:30] if self.title else 'None'}...)>"

    def to_dict(self):
        return {
            "rowid": self.rowid,
            "date": self.date.isoformat() if self.date else None,
            "archive": self.archive,
            "image": self.image,
            "title": self.title,
            "content": self.content,
            "tag": self.tag,
            "link": self.link,
        }
