"""empty message

Revision ID: 270511015a09
Revises: 2f2c88e7db97
Create Date: 2020-04-21 19:44:43.588528

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '270511015a09'
down_revision = '2f2c88e7db97'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('dashboards', sa.Column('additional_filters', sa.String(), nullable=True))
    op.add_column('dashboards', sa.Column('analysis_filters', sa.String(), nullable=True))
    op.add_column('dashboards', sa.Column('compare_filters', sa.String(), nullable=True))
    op.add_column('dashboards', sa.Column('filters', sa.String(), nullable=True))
    op.add_column('dashboards', sa.Column('hash_code', sa.String(), nullable=True))
    op.add_column('dashboards', sa.Column('instances', sa.JSON(), nullable=True))
    op.add_column('dashboards', sa.Column('properties', sa.String(), nullable=True))
    op.add_column('dashboards', sa.Column('timestamp', sa.String(), nullable=True))
    op.create_unique_constraint(None, 'dashboards', ['hash_code'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'dashboards', type_='unique')
    op.drop_column('dashboards', 'timestamp')
    op.drop_column('dashboards', 'properties')
    op.drop_column('dashboards', 'instances')
    op.drop_column('dashboards', 'hash_code')
    op.drop_column('dashboards', 'filters')
    op.drop_column('dashboards', 'compare_filters')
    op.drop_column('dashboards', 'analysis_filters')
    op.drop_column('dashboards', 'additional_filters')
    # ### end Alembic commands ###
