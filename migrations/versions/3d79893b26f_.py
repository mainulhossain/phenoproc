"""empty message

Revision ID: 3d79893b26f
Revises: 1c2b915b8b5
Create Date: 2016-12-16 00:39:39.374300

"""

# revision identifiers, used by Alembic.
revision = '3d79893b26f'
down_revision = '1c2b915b8b5'

from alembic import op
import sqlalchemy as sa


def upgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.add_column('workitems', sa.Column('operation_id', sa.Integer(), nullable=True))
    ### end Alembic commands ###


def downgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('workitems', 'operation_id')
    ### end Alembic commands ###
