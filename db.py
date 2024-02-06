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
                     Column('signature', Text, nullable=False),
                     Column('call_hash', String(255),
                            nullable=False, primary_key=True),
                     Column('status', Integer, nullable=False, default=0,
                            comment='0:未执行 1:执行中 2:已执行且成功 3:已执行且失败'),
                     Column('server_type', Integer, nullable=False, default=0,
                            comment='服务类别'),
                     Column('failure_reason,', String(255), nullable=True,
                            comment='执行失败原因'),
                     Column('create_time', DateTime, nullable=False,
                            comment='创建时间'),
                     Column('update_time', DateTime, nullable=True,
                            comment='更新时间'),
                     Column('signed_time', DateTime, nullable=True,
                            comment='已执行时间'),
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

    # 获取所有未执行的签名
    def get_all_nonexec_signeds(self):
        se = self.proxy_signed_table.select().where(
            self.proxy_signed_table.c.status == 0)
        result = self.session.execute(se).fetchall()
        return result

    # 通过call_hash获取某签名
    def get_signed(self, call_hash: str):
        se = self.proxy_signed_table.select().where(
            self.proxy_signed_table.c.call_hash == call_hash)
        result = self.session.execute(se).fetchall()
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
