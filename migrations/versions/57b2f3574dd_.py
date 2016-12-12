"""empty message

Revision ID: 57b2f3574dd
Revises: 242ad37b498
Create Date: 2016-11-29 16:57:10.452326

"""

# revision identifiers, used by Alembic.
revision = '57b2f3574dd'
down_revision = '242ad37b498'

from alembic import op
import sqlalchemy as sa


def upgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.create_table('data',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('data_source', sa.Integer(), nullable=True),
    sa.Column('datatype', sa.Integer(), nullable=True),
    sa.Column('url', sa.Text(), nullable=True),
    sa.ForeignKeyConstraint(['data_source'], ['datasources.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    ### end Alembic commands ###


def downgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('data')
    ### end Alembic commands ###