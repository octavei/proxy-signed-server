from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, Column, Integer, String, Text, \
    MetaData, Table,  DateTime, UniqueConstraint

from sqlalchemy.orm import Session
from sqlalchemy.dialects.mysql import insert


class ProxySignedDB:
    def __init__(self, db_url: str):
        self.engine = create_engine(db_url, echo=True)
        self.metadata = MetaData()
        self.session = Session(bind=self.engine)
        self.proxy_signed_table = self._proxy_signed_table()
        self.session.commit()
        self.metadata.create_all(bind=self.engine)

    # 创建数据表
    def _proxy_signed_table(self):
        return Table("proxy_signed", self.metadata,
                     Column('id', Integer, autoincrement=True, primary_key=True),
                     Column('call', String(5000), nullable=False),
                     Column('call_hash', String(66),
                            nullable=False, primary_key=True),
                     Column('sign', String(5000), nullable=True),
                     Column('status', Integer, nullable=False, default=0,
                            comment='0:初始化 1:已提交 2:已执行 3:失败'),
                     Column('reason', String(50), nullable=True),
                     Column('exec_height', Integer, nullable=True),
                     Column('create_time', DateTime, nullable=False),
                     Column('tx_hash', String(66), nullable=True),
                     Column('block_num', Integer, nullable=True),
                     Column('tx_id', String(66), nullable=True),
                     Column('block_hash', String(66), nullable=True),
                     UniqueConstraint("call_hash"),
                     extend_existing=True
                     )

    # 插入或更新签名
    def insert_or_update_signed(self, signed_infos: list[dict]):
        try:
            with self.session.begin_nested():
                for signed_info in signed_infos:
                    stmt = insert(self.proxy_signed_table).values(
                        **signed_info)
                    stmt = stmt.on_duplicate_key_update(signed_info)
                    self.session.execute(stmt)
        except SQLAlchemyError as e:
            raise e

    # 获取所有未签名
    def get_all_no_sign(self):
        c = self.proxy_signed_table.c
        se = self.proxy_signed_table.select().where(
            c.status == 0)
        result = self.session.execute(se).fetchall()
        return result

    # 获取所有未执行且到达执行高度的
    def get_all_can_exec(self, now_height):
        c = self.proxy_signed_table.c
        se = self.proxy_signed_table.select().where(
            c.status == 1 and c.exec_height <= now_height)
        result = self.session.execute(se).fetchall()
        return result

    # 通过call_hash获取某签名
    def get_signed(self, call_hash: str):
        se = self.proxy_signed_table.select().where(
            self.proxy_signed_table.c.call_hash == call_hash)
        result = self.session.execute(se).fetchone()
        return result

    # 删除签名
    def delete_signed(self, call_hash: str):
        try:
            with self.session.begin_nested():
                stmt = self.proxy_signed_table.delete().where(
                    self.proxy_signed_table.c.call_hash == call_hash)
                self.session.execute(stmt)
        except SQLAlchemyError as e:
            raise e
