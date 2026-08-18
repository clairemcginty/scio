/*
 * Copyright 2026 Spotify AB
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

package com.spotify.scio.parquet.avro

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.{AvroReadSupport, AvroWriteSupport}
import org.apache.parquet.conf.{HadoopParquetConfiguration, ParquetConfiguration}
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.hadoop.util.ConfigurationUtil
import org.apache.parquet.schema.{GroupType, LogicalTypeAnnotation, MessageType, Type}
import org.slf4j.LoggerFactory

import java.util.{Map => JMap}
import scala.jdk.CollectionConverters._

/**
 * Avro read support that detects the list encoding used by a file when applying projections.
 *
 * Parquet converts a requested Avro projection back to a Parquet schema using the configured list
 * encoding, which defaults to the legacy structure. That projection does not match files using the
 * standard three-level `list/element` structure. When no encoding was explicitly configured, this
 * read support compares the list structures of projected fields present in the file and retries
 * with the standard three-level encoding if necessary. Projected fields absent from the file are
 * ignored during encoding detection so that Avro schema evolution remains supported.
 */
final private[scio] class ScioAvroReadSupport[T] extends AvroReadSupport[T] {
  @transient private lazy val logger = LoggerFactory.getLogger(classOf[ScioAvroReadSupport[_]])

  override def init(
    configuration: ParquetConfiguration,
    keyValueMetaData: JMap[String, String],
    fileSchema: MessageType
  ): ReadContext = {
    val readContext = super.init(configuration, keyValueMetaData, fileSchema)

    if (
      configuration.get(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE) != null ||
      hasCompatibleListEncodings(fileSchema, readContext.getRequestedSchema)
    ) {
      readContext
    } else {
      val adjusted: Configuration = ConfigurationUtil.createHadoopConfiguration(configuration)
      adjusted.setBoolean(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, false)
      val threeLevelReadContext = super.init(
        new HadoopParquetConfiguration(adjusted),
        keyValueMetaData,
        fileSchema
      )
      if (hasCompatibleListEncodings(fileSchema, threeLevelReadContext.getRequestedSchema)) {
        threeLevelReadContext
      } else {
        logger.warn(
          "The requested projection contains list encodings that are incompatible with the " +
            s"file's write schema <$fileSchema>, using either the legacy or standard three-level encoding." +
            "Returned list data may be incorrect."
        )
        readContext
      }
    }
  }

  private def hasCompatibleListEncodings(
    fileSchema: GroupType,
    requestedSchema: GroupType
  ): Boolean =
    requestedSchema.getFields.asScala.forall { requestedField =>
      !fileSchema.containsField(requestedField.getName) ||
      hasCompatibleListEncodings(fileSchema.getType(requestedField.getName), requestedField)
    }

  private def hasCompatibleListEncodings(fileType: Type, requestedType: Type): Boolean = {
    val fileIsList = isList(fileType)
    val requestedIsList = isList(requestedType)

    if (fileIsList || requestedIsList) {
      fileIsList && requestedIsList &&
      hasCompatibleListStructure(fileType.asGroupType(), requestedType.asGroupType())
    } else if (fileType.isPrimitive || requestedType.isPrimitive) {
      true
    } else {
      hasCompatibleListEncodings(fileType.asGroupType(), requestedType.asGroupType())
    }
  }

  private def hasCompatibleListStructure(fileList: GroupType, requestedList: GroupType): Boolean = {
    if (fileList.getFieldCount != 1 || requestedList.getFieldCount != 1) {
      false
    } else {
      val fileRepeatedType = fileList.getType(0)
      val requestedRepeatedType = requestedList.getType(0)

      fileRepeatedType.getName == requestedRepeatedType.getName &&
      hasCompatibleListEncodings(fileRepeatedType, requestedRepeatedType)
    }
  }

  private def isList(t: Type): Boolean =
    !t.isPrimitive &&
      t.asGroupType().getLogicalTypeAnnotation == LogicalTypeAnnotation.listType()
}
