/*
 * Copyright 2026 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.parquet

import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.TapSpec
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.parquet.avro._
import com.spotify.scio.parquet.read.ParquetReadConfiguration
import com.spotify.scio.parquet.types._
import magnolify.parquet.{ArrayEncoding, MagnolifyParquetProperties, ParquetType}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericRecord, GenericRecordBuilder}
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroWriteSupport
import org.scalatest.prop.TableDrivenPropertyChecks.{forAll => forAllCases, Table}

import scala.jdk.CollectionConverters._
import java.io.File

private case class TestRecordScala(
  int_field: Option[Int],
  long_field: Option[Long],
  float_field: Option[Float],
  double_field: Option[Double],
  boolean_field: Option[Boolean],
  string_field: Option[String],
  array_field: List[String]
)

class ParquetFormatInteropTest extends PipelineSpec with TapSpec {

  @scala.annotation.nowarn("cat=unused-privates")
  private def createConfig(splittable: Boolean): Configuration = {
    val c = ParquetConfiguration.empty()
    c.set(ParquetReadConfiguration.UseSplittableDoFn, splittable.toString)
    c
  }
  private val readConfigs =
    Table(
      ("config", "description"),
      (
        () => ParquetConfiguration.of(ParquetReadConfiguration.UseSplittableDoFn -> false),
        "legacy read"
      ),
      (
        () => ParquetConfiguration.of(ParquetReadConfiguration.UseSplittableDoFn -> true),
        "splittable"
      )
    )

  private val specificRecords = (1 to 10).map(AvroUtils.newSpecificRecord)

  private val genericRecords: Seq[GenericRecord] = (1 to 10).map { i =>
    val record = new GenericData.Record(TestRecord.getClassSchema)
    record.put("int_field", i)
    record.put("long_field", i.toLong)
    record.put("float_field", i.toFloat)
    record.put("double_field", i.toDouble)
    record.put("boolean_field", true)
    record.put("string_field", "hello")
    record.put("array_field", List[CharSequence]("a", "b", "c").asJava)
    record
  }

  private val typedRecords = (1 to 10).map { i =>
    TestRecordScala(
      Some(i),
      Some(i.toLong),
      Some(i.toFloat),
      Some(i.toDouble),
      Some(true),
      Some("hello"),
      List("a", "b", "c")
    )
  }

  private val avroProjectionSchema: Schema = SchemaBuilder
    .record("TestRecordProjection")
    .fields()
    .nullableInt("int_field", 0)
    .name("array_field")
    .`type`(SchemaBuilder.array().items(Schema.create(Schema.Type.STRING)))
    .noDefault()
    .endRecord()

  private val avroProjectionWithMissingFieldSchema: Schema = SchemaBuilder
    .record("TestRecordProjectionWithMissingField")
    .fields()
    .nullableInt("int_field", 0)
    .name("array_field")
    .`type`(SchemaBuilder.array().items(Schema.create(Schema.Type.STRING)))
    .noDefault()
    .optionalString("missing_field")
    .endRecord()

  val grCoder: Coder[GenericRecord] = avroGenericRecordCoder(TestRecord.getClassSchema)
  val grProjectionCoder: Coder[GenericRecord] = avroGenericRecordCoder(avroProjectionSchema)
  val grProjectionWithMissingFieldCoder: Coder[GenericRecord] =
    avroGenericRecordCoder(avroProjectionWithMissingFieldSchema)

  private val ptUngroupedListEncoding = ParquetType[TestRecordScala](
    new MagnolifyParquetProperties {
      override def writeArrayEncoding: ArrayEncoding = ArrayEncoding.Ungrouped
    }
  )

  private val ptOldListEncoding = ParquetType[TestRecordScala](
    new MagnolifyParquetProperties {
      override def writeArrayEncoding: ArrayEncoding = ArrayEncoding.ThreeLevelArray
    }
  )

  private val ptNewListEncoding = ParquetType[TestRecordScala](
    new MagnolifyParquetProperties {
      override def writeArrayEncoding: ArrayEncoding = ArrayEncoding.ThreeLevelList
    }
  )

  ".typedParquetFile" should "be able to read data written with .saveAsParquetAvroFile with old list encoding" in withTempDir {
    dir =>
      implicit val pt: ParquetType[TestRecordScala] = ptOldListEncoding

      runWithRealContext()(
        _.parallelize(specificRecords)
          .saveAsParquetAvroFile(dir.toString)
      )

      forAllCases(readConfigs) { case (c, _) =>
        runWithRealContext()(
          _.typedParquetFile[TestRecordScala](s"$dir/*.parquet", conf = c())
            .map(identity) should containInAnyOrder(typedRecords)
        )
      }
  }

  it should "be able to read data written with .saveAsParquetAvroFile with new list encoding" in withTempDir {
    dir =>
      implicit val pt: ParquetType[TestRecordScala] = ptNewListEncoding
      val listConf = ParquetConfiguration.of(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE -> false)

      runWithRealContext()(
        _.parallelize(specificRecords)
          .saveAsParquetAvroFile(dir.toString, conf = listConf)
      )

      forAllCases(readConfigs) { case (c, _) =>
        runWithRealContext()(
          _.typedParquetFile[TestRecordScala](s"$dir/*.parquet", conf = c())
            .map(identity) should containInAnyOrder(typedRecords)
        )
      }
  }

  def testFailOnMismatchedReadWriteEncodings(
    writeEncoding: ParquetType[TestRecordScala],
    readEncoding: ParquetType[TestRecordScala],
    dir: File
  ): Unit = {
    {
      implicit val ptWrite: ParquetType[TestRecordScala] = writeEncoding
      runWithRealContext()(
        _.parallelize(typedRecords)
          .saveAsTypedParquetFile(dir.toString)
      )
    }
    {
      val e = the[PipelineExecutionException] thrownBy {
        implicit val ptRead: ParquetType[TestRecordScala] = readEncoding
        runWithRealContext()(
          _.typedParquetFile[TestRecordScala](s"$dir/*.parquet")
            .map(identity) should containInAnyOrder(typedRecords)
        )
      }
      e.getCause.getClass shouldBe classOf[org.apache.parquet.io.InvalidRecordException]
    }
  }

  it should "fail quickly when reading data written with new list encoding if read ArrayEncoding is set to 2 level" in withTempDir {
    dir =>
      testFailOnMismatchedReadWriteEncodings(ptNewListEncoding, ptOldListEncoding, dir)
  }

  it should "fail quickly when reading data written with new list encoding if read ArrayEncoding is set to ungrouped" in withTempDir {
    dir =>
      testFailOnMismatchedReadWriteEncodings(ptNewListEncoding, ptUngroupedListEncoding, dir)
  }

  it should "fail quickly when reading data written with old list encoding if read ArrayEncoding is set to new encoding" in withTempDir {
    dir =>
      testFailOnMismatchedReadWriteEncodings(ptOldListEncoding, ptNewListEncoding, dir)
  }

  it should "fail quickly when reading data written with old list encoding if read ArrayEncoding is set to ungrouped" in withTempDir {
    dir =>
      testFailOnMismatchedReadWriteEncodings(ptOldListEncoding, ptUngroupedListEncoding, dir)
  }

  ".parquetAvroFile" should "be able to read GenericRecord data written with .saveAsTypedParquetFile with old list encoding" in withTempDir {
    dir =>
      implicit val pt: ParquetType[TestRecordScala] = ptOldListEncoding
      implicit val coder: Coder[GenericRecord] = grCoder

      runWithRealContext()(
        _.parallelize(typedRecords)
          .saveAsTypedParquetFile(dir.toString)
      )

      forAllCases(readConfigs) { case (c, _) =>
        runWithRealContext()(
          _.parquetAvroFile[GenericRecord](
            s"$dir/*.parquet",
            projection = TestRecord.getClassSchema,
            conf = c()
          )
            .map(identity) should containInAnyOrder(genericRecords)
        )
      }
  }

  it should "be able to read GenericRecord data written with .saveAsTypedParquetFile with new list encoding" in withTempDir {
    dir =>
      implicit val pt: ParquetType[TestRecordScala] = ptNewListEncoding
      implicit val coder: Coder[GenericRecord] = grCoder

      runWithRealContext()(
        _.parallelize(typedRecords)
          .saveAsTypedParquetFile(dir.toString)
      )

      forAllCases(readConfigs) { case (c, _) =>
        runWithRealContext()(
          _.parquetAvroFile[GenericRecord](
            s"$dir/*.parquet",
            projection = TestRecord.getClassSchema,
            conf = c()
          )
            .map(identity) should containInAnyOrder(genericRecords)
        )
      }
  }

  it should "be able to read GenericRecord data written with .saveAsTypedParquetFile with new list encoding and a projected list schema" in withTempDir {
    dir =>
      implicit val pt: ParquetType[TestRecordScala] = ptNewListEncoding
      implicit val coder: Coder[GenericRecord] = grProjectionCoder

      runWithRealContext()(
        _.parallelize(typedRecords)
          .saveAsTypedParquetFile(dir.toString)
      )

      forAllCases(readConfigs) { case (c, _) =>
        runWithRealContext()(
          _.parquetAvroFile[GenericRecord](
            s"$dir/*.parquet",
            projection = avroProjectionSchema,
            conf = c()
          )
            .map(identity) should containInAnyOrder(genericRecords.map { gr =>
            new GenericRecordBuilder(avroProjectionSchema)
              .set("array_field", gr.get("array_field"))
              .set("int_field", gr.get("int_field"))
              .build()
              .asInstanceOf[GenericRecord]
          })
        )
      }
  }

  it should "detect new list encoding when the projection contains a missing optional field" in withTempDir {
    dir =>
      implicit val pt: ParquetType[TestRecordScala] = ptNewListEncoding
      implicit val coder: Coder[GenericRecord] = grProjectionWithMissingFieldCoder

      runWithRealContext()(
        _.parallelize(typedRecords)
          .saveAsTypedParquetFile(dir.toString)
      )

      forAllCases(readConfigs) { case (c, _) =>
        runWithRealContext()(
          _.parquetAvroFile[GenericRecord](
            s"$dir/*.parquet",
            projection = avroProjectionWithMissingFieldSchema,
            conf = c()
          )
            .map(identity) should containInAnyOrder(genericRecords.map { gr =>
            new GenericRecordBuilder(avroProjectionWithMissingFieldSchema)
              .set("array_field", gr.get("array_field"))
              .set("int_field", gr.get("int_field"))
              .build()
              .asInstanceOf[GenericRecord]
          })
        )
      }
  }

  it should "be able to read SpecificRecord data written with .saveAsTypedParquetFile with old list encoding" in withTempDir {
    dir =>
      implicit val pt: ParquetType[TestRecordScala] = ptOldListEncoding

      runWithRealContext()(
        _.parallelize(typedRecords)
          .saveAsTypedParquetFile(dir.toString)
      )

      forAllCases(readConfigs) { case (c, _) =>
        runWithRealContext()(
          _.parquetAvroFile[TestRecord](s"$dir/*.parquet", conf = c())
            .map(identity) should containInAnyOrder(specificRecords)
        )
      }
  }

  it should "be able to read SpecificRecord data written with .saveAsTypedParquetFile with new list encoding" in withTempDir {
    dir =>
      implicit val pt: ParquetType[TestRecordScala] = ptNewListEncoding

      runWithRealContext()(
        _.parallelize(typedRecords)
          .saveAsTypedParquetFile(dir.toString)
      )

      forAllCases(readConfigs) { case (c, _) =>
        runWithRealContext()(
          _.parquetAvroFile[TestRecord](s"$dir/*.parquet", conf = c())
            .map(identity) should containInAnyOrder(specificRecords)
        )
      }
  }

  it should "be able to read SpecificRecord data written with .saveAsTypedParquetFile with new list encoding and a projected list schema" in withTempDir {
    dir =>
      implicit val pt: ParquetType[TestRecordScala] = ptNewListEncoding

      runWithRealContext()(
        _.parallelize(typedRecords)
          .saveAsTypedParquetFile(dir.toString)
      )

      forAllCases(readConfigs) { case (c, _) =>
        runWithRealContext()(
          _.parquetAvroFile[TestRecord](
            s"$dir/*.parquet",
            conf = c(),
            projection = avroProjectionSchema
          )
            .map(identity) should containInAnyOrder(specificRecords.map { sr =>
            TestRecord.newBuilder().setArrayField(sr.array_field).setIntField(sr.int_field).build()
          })
        )
      }
  }
}
