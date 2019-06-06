package com.acc.datalake.dataclean.udf

object UDF {

  def strLen(str: String) = {
    if (str != null) str.length
    else 0
  }

}
