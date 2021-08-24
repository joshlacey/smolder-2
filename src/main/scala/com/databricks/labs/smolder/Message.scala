/*
 * Copyright 2020 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.labs.smolder

import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import scala.collection.JavaConversions._

private[smolder] object Message {

  /** @return
    *   The Spark SQL schema for an HL7 message.
    */
  def schema: StructType = StructType(
    Seq(
      StructField("message", StringType),
      StructField(
        "segments",
        ArrayType(
          StructType(
            Seq(
              StructField("id", StringType),
              StructField("fields", ArrayType(StringType, false))
            )
          ),
          false
        )
      ),
      StructField("raw_file_path", StringType)
    )
  )

  /** Parses HL7 messages from an iterator over strings.
    *
    * @param lines
    *   An iterator containing all the lines from an HL7 message.
    * @return
    *   Parses the message into a Message case class.
    */
  def apply(
      lines: Iterator[String],
      includeMSHInSegments: Boolean = false,
      rawPath: String
  ): Message = {
    require(lines.hasNext, "Received empty message.")
    if (includeMSHInSegments) {
      Message(
        UTF8String.fromString("MSH included in Segments Column"),
        lines.toSeq
          .map(Segment(_)),
        UTF8String.fromString(rawPath)
      )
    } else {
      Message(
        UTF8String.fromString(lines.next),
        lines.toSeq
          .map(Segment(_)),
        UTF8String.fromString(rawPath)
      )
    }
  }

  /** Parses HL7 messages from a string.
    *
    * Returns a null if the input is null.
    *
    * @param text
    *   A string to parse.
    * @return
    *   Parses the message into a Message case class.
    */
  def apply(text: UTF8String): Message = {

    val delim: Byte = 0x0d

    if (text == null) {
      null
    } else {
      val textString = text.toString
      require(textString.nonEmpty, "Received empty string.")

      Message(
        textString.split(delim.toChar).toIterator,
        false,
        ""
      )
    }
  }

  /** Parses HL7 messages from a string.
    *
    * Returns a null if the input is null.
    *
    * @param text
    *   A string to parse.
    * @param includeMSHInSegments
    *   tell the parser to allow the MSH to be included in the segments so that
    *   they can be queried like anything else.
    * @return
    *   Parses the message into a Message case class.
    */
  def apply(
      text: UTF8String,
      includeMSHInSegments: Boolean,
      rawPath: String
  ): Message = {

    val delim: Byte = 0x0d

    if (text == null) {
      null
    } else {
      val textString = text.toString
      require(textString.nonEmpty, "Received empty string.")

      Message(
        textString.split(delim.toChar).toIterator,
        includeMSHInSegments,
        ""
      )
    }
  }
}

/** Convenience class for parsing HL7 messages into Spark SQL Rows.
  *
  * @param message
  *   The message segment header text.
  * @param segments
  *   The segments contained within this message.
  * @param raw_file_path
  *   The raw file path for the original hl7 message
  */
private[smolder] case class Message(
    message: UTF8String,
    segments: Seq[Segment],
    raw_file_path: UTF8String
) {

  /** Returns a message as a row, with all possible fields included.
    *
    * @return
    *   Converts into a Spark SQL InternalRow.
    */
  def toInternalRow(): InternalRow = {
    toInternalRow(Message.schema)
  }

  /** Returns a message as a row, possibly with some fields projected away.
    *
    * @param requiredSchema
    *   The schema to project.
    * @return
    *   Converts into a Spark SQL InternalRow.
    */
  def toInternalRow(requiredSchema: StructType): InternalRow = {
    def makeSegments: ArrayData = {
      ArrayData.toArrayData(
        segments
          .map(s => {
            s.toInternalRow(
              requiredSchema("segments").dataType
                .asInstanceOf[ArrayType]
                .elementType
                .asInstanceOf[StructType]
            )
          })
          .toArray
      )
    }
    val fieldNames = requiredSchema.fieldNames
    val messageFields = (
      fieldNames.contains("message"),
      fieldNames.contains("segments"),
      fieldNames.contains("raw_file_path")
    )

    messageFields match {
      case (true, true, true) =>
        InternalRow(message, makeSegments, raw_file_path)
      case (true, false, false) => InternalRow(message)
      case (false, true, false) => InternalRow(makeSegments)
      case (_, _, _)            => InternalRow()
    }
  }
}
