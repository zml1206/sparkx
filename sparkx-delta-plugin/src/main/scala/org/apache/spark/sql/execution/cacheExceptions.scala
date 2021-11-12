package org.apache.spark.sql.execution

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier

/**
 * Created by zhuml on 2021/11/8.
 */
class PartitionFilterNotExistException(ident: TableIdentifier)
  extends AnalysisException(s"${ident.unquotedString} 是分区表，需要添加分区过滤进行查询")
