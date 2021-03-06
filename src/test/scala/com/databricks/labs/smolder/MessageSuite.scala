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

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import scala.io.Source

class MessageSuite extends SmolderBaseTest {

  val msh = "MSH|^~\\&|||||20201020150800.739+0000||ADT^A03^ADT_A03|11374301|P|2.4"

  test("validate schema") {
    val schema = Message.schema

    assert(schema.size === 3)
    assert(schema("message").dataType === StringType)
    assert(schema("segments").dataType match {
      case ArrayType(_, false) => true
      case _ => false
    })

    val segmentSchema = schema("segments").dataType match {
      case ArrayType(structType: StructType, _) => structType
    }
    assert(segmentSchema.size === 2)
    assert(segmentSchema("id").dataType === StringType)
    assert(segmentSchema("fields").dataType match {
      case ArrayType(StringType, false) => true
      case _ => false
    })
  }

  test("cannot parse an empty iterator") {
    intercept[IllegalArgumentException] {
      Message(Iterator(), "", "")
    }
  }

  test("cannot parse an empty string") {
    intercept[IllegalArgumentException] {
      Message(UTF8String.fromString(""), "", "")
    }
  }

  test("passing a null string returns a null message") {
    val nullMessage: UTF8String = null
    assert(Message(nullMessage, "", "") === null)
  }

  test("parse a full message, by iterator") {
    val delim: Byte = 0x0d
    val file = testFile("single_record.hl7")
    val lines: String = Source.fromFile(file).getLines().mkString(delim.toChar.toString)
    val message = Message(UTF8String.fromString(lines), "", lines)
    val msg: String = message.message.toString()
    assert(msg === lines)

    val segments = message.segments
    assert(segments.size === 4)

    def validateSegment(idx: Int, id: String, size: Int) {
      assert(segments(idx).id.toString === id)
      assert(segments(idx).fields.size === size)
    }

    validateSegment(0, "MSH", 11)
    validateSegment(1, "EVN", 2)
    validateSegment(2, "PID", 11)
    validateSegment(3, "PV1", 44)
  }

  test("parse a full message, by string") {

    val delim: Byte = 0x0d

    val file = testFile("single_record.hl7")
    val lines: String = Source.fromFile(file).getLines().mkString(delim.toChar.toString)
    val message = Message(UTF8String.fromString(lines), "", lines)
    val msg: String = message.message.toString()
    assert(msg === lines)

    val segments = message.segments
    assert(segments.size === 4)

    def validateSegment(idx: Int, id: String, size: Int) {
      assert(segments(idx).id.toString === id)
      assert(segments(idx).fields.size === size)
    }

    validateSegment(0, "MSH", 11)
    validateSegment(1, "EVN", 2)
    validateSegment(2, "PID", 11)
    validateSegment(3, "PV1", 44)
  }

  test("can inclue message in the Segment column 126") {
    
    val delim: Byte = 0x0d // \r

    val file = testFile("single_record.hl7")
    val lines: String = Source.fromFile(file).getLines().mkString(delim.toChar.toString)
    val message = Message(UTF8String.fromString(lines), "", lines)
    val msg: String = message.message.toString()
    assert(msg === lines)

    val segments = message.segments
    assert(segments.size === 4)

    def validateSegment(idx: Int, id: String, size: Int) {
      assert(segments(idx).id.toString === id)
      assert(segments(idx).fields.size === size)
    }

    validateSegment(0, "MSH", 11)
    validateSegment(1, "EVN", 2)
    validateSegment(2, "PID", 11)
    validateSegment(3, "PV1", 44)
  }
}
