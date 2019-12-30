/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import java.io._
import java.util.Properties

import org.apache.kafka.common.errors.{CorruptRecordException, OffsetOutOfRangeException, RecordBatchTooLargeException, RecordTooLargeException}
import kafka.api.ApiVersion
import kafka.common.LongRef
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite
import org.junit.{After, Before, Test}
import kafka.message._
import kafka.utils._
import kafka.server.KafkaConfig
import org.apache.kafka.common.utils.Utils

class LogTest extends JUnitSuite {

  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val time = new MockTime(0)
  var config: KafkaConfig = null
  val logConfig = LogConfig()

  @Before
  def setUp() {
    val props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1)
    config = KafkaConfig.fromProps(props)
  }

  @After
  def tearDown() {
    Utils.delete(tmpDir)
  }

  def createEmptyLogs(dir: File, offsets: Int*) {
    for(offset <- offsets) {
      Log.logFilename(dir, offset).createNewFile()
      Log.indexFilename(dir, offset).createNewFile()
    }
  }

  /**
   * Tests for time based log roll. This test appends messages then changes the time
   * using the mock clock to force the log to roll and checks the number of segments.
   */
  @Test
  def testTimeBasedLogRoll() {
    val set = TestUtils.singleMessageSet("test".getBytes)

    val logProps = new Properties()
    logProps.put(LogConfig.SegmentMsProp, (1 * 60 * 60L): java.lang.Long)

    // create a log
    val log = new Log(logDir,
                      LogConfig(logProps),
                      recoveryPoint = 0L,
                      scheduler = time.scheduler,
                      time = time)
    assertEquals("Log begins with a single empty segment.", 1, log.numberOfSegments)
    time.sleep(log.config.segmentMs + 1)
    log.append(set)
    assertEquals("Log doesn't roll if doing so creates an empty segment.", 1, log.numberOfSegments)

    log.append(set)
    assertEquals("Log rolls on this append since time has expired.", 2, log.numberOfSegments)

    for(numSegments <- 3 until 5) {
      time.sleep(log.config.segmentMs + 1)
      log.append(set)
      assertEquals("Changing time beyond rollMs and appending should create a new segment.", numSegments, log.numberOfSegments)
    }

    val numSegments = log.numberOfSegments
    time.sleep(log.config.segmentMs + 1)
    log.append(new ByteBufferMessageSet())
    assertEquals("Appending an empty message set should not roll log even if succient time has passed.", numSegments, log.numberOfSegments)
  }

  /**
   * Test for jitter s for time based log roll. This test appends messages then changes the time
   * using the mock clock to force the log to roll and checks the number of segments.
   */
  @Test
  def testTimeBasedLogRollJitter() {
    val set = TestUtils.singleMessageSet("test".getBytes)
    val maxJitter = 20 * 60L

    val logProps = new Properties()
    logProps.put(LogConfig.SegmentMsProp, 1 * 60 * 60: java.lang.Long)
    logProps.put(LogConfig.SegmentJitterMsProp, maxJitter: java.lang.Long)
    // create a log
    val log = new Log(logDir,
      LogConfig(logProps),
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      time = time)
    assertEquals("Log begins with a single empty segment.", 1, log.numberOfSegments)
    log.append(set)

    time.sleep(log.config.segmentMs - maxJitter)
    log.append(set)
    assertEquals("Log does not roll on this append because it occurs earlier than max jitter", 1, log.numberOfSegments)
    time.sleep(maxJitter - log.activeSegment.rollJitterMs + 1)
    log.append(set)
    assertEquals("Log should roll after segmentMs adjusted by random jitter", 2, log.numberOfSegments)
  }

  /**
   * Test that appending more than the maximum segment size rolls the log
   */
  @Test
  def testSizeBasedLogRoll() {
    val set = TestUtils.singleMessageSet("test".getBytes)
    val setSize = set.sizeInBytes
    val msgPerSeg = 10
    val segmentSize = msgPerSeg * (setSize - 1) // each segment will be 10 messages

    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, segmentSize: java.lang.Integer)
    // We use need to use magic value 1 here because the test is message size sensitive.
    logProps.put(LogConfig.MessageFormatVersionProp, ApiVersion.latestVersion.toString)
    // create a log
    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)

    // segments expire in size
    for (i<- 1 to (msgPerSeg + 1)) {
      log.append(set)
    }
    assertEquals("There should be exactly 2 segments.", 2, log.numberOfSegments)
  }

  /**
   * Test that we can open and append to an empty log
   */
  @Test
  def testLoadEmptyLog() {
    createEmptyLogs(logDir, 0)
    val log = new Log(logDir, logConfig, recoveryPoint = 0L, time.scheduler, time = time)
    log.append(TestUtils.singleMessageSet("test".getBytes))
  }

  /**
   * This test case appends a bunch of messages and checks that we can read them all back using sequential offsets.
   */
  @Test
  def testAppendAndReadWithSequentialOffsets() {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 71: java.lang.Integer)
    // We use need to use magic value 1 here because the test is message size sensitive.
    logProps.put(LogConfig.MessageFormatVersionProp, ApiVersion.latestVersion.toString)
    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)
    val messages = (0 until 100 by 2).map(id => new Message(id.toString.getBytes)).toArray

    for(i <- 0 until messages.length)
      log.append(new ByteBufferMessageSet(NoCompressionCodec, messages = messages(i)))
    for(i <- 0 until messages.length) {
      val read = log.read(i, 100, Some(i+1)).messageSet.head
      assertEquals("Offset read should match order appended.", i, read.offset)
      assertEquals("Message should match appended.", messages(i), read.message)
    }
    assertEquals("Reading beyond the last message returns nothing.", 0, log.read(messages.length, 100, None).messageSet.size)
  }

  /**
   * This test appends a bunch of messages with non-sequential offsets and checks that we can read the correct message
   * from any offset less than the logEndOffset including offsets not appended.
   */
  @Test
  def testAppendAndReadWithNonSequentialOffsets() {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 71: java.lang.Integer)
    val log = new Log(logDir,  LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)
    val messageIds = ((0 until 50) ++ (50 until 200 by 7)).toArray
    val messages = messageIds.map(id => new Message(id.toString.getBytes))

    // now test the case that we give the offsets and use non-sequential offsets
    for(i <- 0 until messages.length)
      log.append(new ByteBufferMessageSet(NoCompressionCodec, new LongRef(messageIds(i)), messages = messages(i)), assignOffsets = false)
    for(i <- 50 until messageIds.max) {
      val idx = messageIds.indexWhere(_ >= i)
      val read = log.read(i, 100, None).messageSet.head
      assertEquals("Offset read should match message id.", messageIds(idx), read.offset)
      assertEquals("Message should match appended.", messages(idx), read.message)
    }
  }

  /**
   * This test covers an odd case where we have a gap in the offsets that falls at the end of a log segment.
   * Specifically we create a log where the last message in the first segment has offset 0. If we
   * then read offset 1, we should expect this read to come from the second segment, even though the
   * first segment has the greatest lower bound on the offset.
   */
  @Test
  def testReadAtLogGap() {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 300: java.lang.Integer)
    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)

    // keep appending until we have two segments with only a single message in the second segment
    while(log.numberOfSegments == 1)
      log.append(new ByteBufferMessageSet(NoCompressionCodec, messages = new Message("42".getBytes)))

    // now manually truncate off all but one message from the first segment to create a gap in the messages
    log.logSegments.head.truncateTo(1)

    assertEquals("A read should now return the last message in the log", log.logEndOffset - 1, log.read(1, 200, None).messageSet.head.offset)
  }

  /**
   * Test reading at the boundary of the log, specifically
   * - reading from the logEndOffset should give an empty message set
   * - reading from the maxOffset should give an empty message set
   * - reading beyond the log end offset should throw an OffsetOutOfRangeException
   */
  @Test
  def testReadOutOfRange() {
    createEmptyLogs(logDir, 1024)
    val logProps = new Properties()

    // set up replica log starting with offset 1024 and with one message (at offset 1024)
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)
    log.append(new ByteBufferMessageSet(NoCompressionCodec, messages = new Message("42".getBytes)))

    assertEquals("Reading at the log end offset should produce 0 byte read.", 0, log.read(1025, 1000).messageSet.sizeInBytes)

    try {
      log.read(0, 1000)
      fail("Reading below the log start offset should throw OffsetOutOfRangeException")
    } catch {
      case e: OffsetOutOfRangeException => // This is good.
    }

    try {
      log.read(1026, 1000)
      fail("Reading at beyond the log end offset should throw OffsetOutOfRangeException")
    } catch {
      case e: OffsetOutOfRangeException => // This is good.
    }

    assertEquals("Reading from below the specified maxOffset should produce 0 byte read.", 0, log.read(1025, 1000, Some(1024)).messageSet.sizeInBytes)
  }

  /**
   * Test that covers reads and writes on a multisegment log. This test appends a bunch of messages
   * and then reads them all back and checks that the message read and offset matches what was appended.
   */
  @Test
  def testLogRolls() {
    /* create a multipart log with 100 messages */
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 100: java.lang.Integer)
    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)
    val numMessages = 100
    val messageSets = (0 until numMessages).map(i => TestUtils.singleMessageSet(i.toString.getBytes))
    messageSets.foreach(log.append(_))
    log.flush

    /* do successive reads to ensure all our messages are there */
    var offset = 0L
    for(i <- 0 until numMessages) {
      val messages = log.read(offset, 1024*1024).messageSet
      assertEquals("Offsets not equal", offset, messages.head.offset)
      assertEquals("Messages not equal at offset " + offset, messageSets(i).head.message,
        messages.head.message.toFormatVersion(messageSets(i).head.message.magic))
      offset = messages.head.offset + 1
    }
    val lastRead = log.read(startOffset = numMessages, maxLength = 1024*1024, maxOffset = Some(numMessages + 1)).messageSet
    assertEquals("Should be no more messages", 0, lastRead.size)

    // check that rolling the log forced a flushed the log--the flush is asyn so retry in case of failure
    TestUtils.retry(1000L){
      assertTrue("Log role should have forced flush", log.recoveryPoint >= log.activeSegment.baseOffset)
    }
  }

  /**
   * Test reads at offsets that fall within compressed message set boundaries.
   */
  @Test
  def testCompressedMessages() {
    /* this log should roll after every messageset */
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 100: java.lang.Integer)
    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)

    /* append 2 compressed message sets, each with two messages giving offsets 0, 1, 2, 3 */
    log.append(new ByteBufferMessageSet(DefaultCompressionCodec, new Message("hello".getBytes), new Message("there".getBytes)))
    log.append(new ByteBufferMessageSet(DefaultCompressionCodec, new Message("alpha".getBytes), new Message("beta".getBytes)))

    def read(offset: Int) = ByteBufferMessageSet.deepIterator(log.read(offset, 4096).messageSet.head)

    /* we should always get the first message in the compressed set when reading any offset in the set */
    assertEquals("Read at offset 0 should produce 0", 0, read(0).next().offset)
    assertEquals("Read at offset 1 should produce 0", 0, read(1).next().offset)
    assertEquals("Read at offset 2 should produce 2", 2, read(2).next().offset)
    assertEquals("Read at offset 3 should produce 2", 2, read(3).next().offset)
  }

  /**
   * Test garbage collecting old segments
   */
  @Test
  def testThatGarbageCollectingSegmentsDoesntChangeOffset() {
    for(messagesToAppend <- List(0, 1, 25)) {
      logDir.mkdirs()
      // first test a log segment starting at 0
      val logProps = new Properties()
      logProps.put(LogConfig.SegmentBytesProp, 100: java.lang.Integer)
      val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)
      for(i <- 0 until messagesToAppend)
        log.append(TestUtils.singleMessageSet(i.toString.getBytes))

      val currOffset = log.logEndOffset
      assertEquals(currOffset, messagesToAppend)

      // time goes by; the log file is deleted
      log.deleteOldSegments(_ => true)

      assertEquals("Deleting segments shouldn't have changed the logEndOffset", currOffset, log.logEndOffset)
      assertEquals("We should still have one segment left", 1, log.numberOfSegments)
      assertEquals("Further collection shouldn't delete anything", 0, log.deleteOldSegments(_ => true))
      assertEquals("Still no change in the logEndOffset", currOffset, log.logEndOffset)
      assertEquals("Should still be able to append and should get the logEndOffset assigned to the new append",
                   currOffset,
                   log.append(TestUtils.singleMessageSet("hello".getBytes)).firstOffset)

      // cleanup the log
      log.delete()
    }
  }

  /**
   *  MessageSet size shouldn't exceed the config.segmentSize, check that it is properly enforced by
   * appending a message set larger than the config.segmentSize setting and checking that an exception is thrown.
   */
  @Test
  def testMessageSetSizeCheck() {
    val messageSet = new ByteBufferMessageSet(NoCompressionCodec, new Message ("You".getBytes), new Message("bethe".getBytes))
    // append messages to log
    val configSegmentSize = messageSet.sizeInBytes - 1
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, configSegmentSize: java.lang.Integer)
    // We use need to use magic value 1 here because the test is message size sensitive.
    logProps.put(LogConfig.MessageFormatVersionProp, ApiVersion.latestVersion.toString)
    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)

    try {
      log.append(messageSet)
      fail("message set should throw RecordBatchTooLargeException.")
    } catch {
      case e: RecordBatchTooLargeException => // this is good
    }
  }

  @Test
  def testCompactedTopicConstraints() {
    val keyedMessage = new Message(bytes = "this message has a key".getBytes, key = "and here it is".getBytes, Message.NoTimestamp, Message.CurrentMagicValue)
    val anotherKeyedMessage = new Message(bytes = "this message also has a key".getBytes, key ="another key".getBytes, Message.NoTimestamp, Message.CurrentMagicValue)
    val unkeyedMessage = new Message(bytes = "this message does not have a key".getBytes)

    val messageSetWithUnkeyedMessage = new ByteBufferMessageSet(NoCompressionCodec, unkeyedMessage, keyedMessage)
    val messageSetWithOneUnkeyedMessage = new ByteBufferMessageSet(NoCompressionCodec, unkeyedMessage)
    val messageSetWithCompressedKeyedMessage = new ByteBufferMessageSet(GZIPCompressionCodec, keyedMessage)
    val messageSetWithCompressedUnkeyedMessage = new ByteBufferMessageSet(GZIPCompressionCodec, keyedMessage, unkeyedMessage)

    val messageSetWithKeyedMessage = new ByteBufferMessageSet(NoCompressionCodec, keyedMessage)
    val messageSetWithKeyedMessages = new ByteBufferMessageSet(NoCompressionCodec, keyedMessage, anotherKeyedMessage)

    val logProps = new Properties()
    logProps.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)

    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time)

    try {
      log.append(messageSetWithUnkeyedMessage)
      fail("Compacted topics cannot accept a message without a key.")
    } catch {
      case e: CorruptRecordException => // this is good
    }
    try {
      log.append(messageSetWithOneUnkeyedMessage)
      fail("Compacted topics cannot accept a message without a key.")
    } catch {
      case e: CorruptRecordException => // this is good
    }
    try {
      log.append(messageSetWithCompressedUnkeyedMessage)
      fail("Compacted topics cannot accept a message without a key.")
    } catch {
      case e: CorruptRecordException => // this is good
    }

    // the following should succeed without any InvalidMessageException
    log.append(messageSetWithKeyedMessage)
    log.append(messageSetWithKeyedMessages)
    log.append(messageSetWithCompressedKeyedMessage)
  }

  /**
   * We have a max size limit on message appends, check that it is properly enforced by appending a message larger than the
   * setting and checking that an exception is thrown.
   */
  @Test
  def testMessageSizeCheck() {
    val first = new ByteBufferMessageSet(NoCompressionCodec, new Message ("You".getBytes), new Message("bethe".getBytes))
    val second = new ByteBufferMessageSet(NoCompressionCodec, new Message("change (I need more bytes)".getBytes))

    // append messages to log
    val maxMessageSize = second.sizeInBytes - 1
    val logProps = new Properties()
    logProps.put(LogConfig.MaxMessageBytesProp, maxMessageSize: java.lang.Integer)
    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, time.scheduler, time = time)

    // should be able to append the small message
    log.append(first)

    try {
      log.append(second)
      fail("Second message set should throw MessageSizeTooLargeException.")
    } catch {
      case e: RecordTooLargeException => // this is good
    }
  }
  /**
   * Append a bunch of messages to a log and then re-open it both with and without recovery and check that the log re-initializes correctly.
   */
  @Test
  def testLogRecoversToCorrectOffset() {
    val numMessages = 100
    val messageSize = 100
    val segmentSize = 7 * messageSize
    val indexInterval = 3 * messageSize
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, segmentSize: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, indexInterval: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 4096: java.lang.Integer)
    val config = LogConfig(logProps)
    var log = new Log(logDir, config, recoveryPoint = 0L, time.scheduler, time)
    for(i <- 0 until numMessages)
      log.append(TestUtils.singleMessageSet(TestUtils.randomBytes(messageSize)))
    assertEquals("After appending %d messages to an empty log, the log end offset should be %d".format(numMessages, numMessages), numMessages, log.logEndOffset)
    val lastIndexOffset = log.activeSegment.index.lastOffset
    val numIndexEntries = log.activeSegment.index.entries
    val lastOffset = log.logEndOffset
    log.close()

    log = new Log(logDir, config, recoveryPoint = lastOffset, time.scheduler, time)
    assertEquals("Should have %d messages when log is reopened w/o recovery".format(numMessages), numMessages, log.logEndOffset)
    assertEquals("Should have same last index offset as before.", lastIndexOffset, log.activeSegment.index.lastOffset)
    assertEquals("Should have same number of index entries as before.", numIndexEntries, log.activeSegment.index.entries)
    log.close()

    // test recovery case
    log = new Log(logDir, config, recoveryPoint = 0L, time.scheduler, time)
    assertEquals("Should have %d messages when log is reopened with recovery".format(numMessages), numMessages, log.logEndOffset)
    assertEquals("Should have same last index offset as before.", lastIndexOffset, log.activeSegment.index.lastOffset)
    assertEquals("Should have same number of index entries as before.", numIndexEntries, log.activeSegment.index.entries)
    log.close()
  }

  /**
   * Test that if we manually delete an index segment it is rebuilt when the log is re-opened
   */
  @Test
  def testIndexRebuild() {
    // publish the messages and close the log
    val numMessages = 200
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 200: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)

    val config = LogConfig(logProps)
    var log = new Log(logDir, config, recoveryPoint = 0L, time.scheduler, time)
    for(i <- 0 until numMessages)
      log.append(TestUtils.singleMessageSet(TestUtils.randomBytes(10)))
    val indexFiles = log.logSegments.map(_.index.file)
    log.close()

    // delete all the index files
    indexFiles.foreach(_.delete())

    // reopen the log
    log = new Log(logDir, config, recoveryPoint = 0L, time.scheduler, time)
    assertEquals("Should have %d messages when log is reopened".format(numMessages), numMessages, log.logEndOffset)
    for(i <- 0 until numMessages)
      assertEquals(i, log.read(i, 100, None).messageSet.head.offset)
    log.close()
  }

  /**
   * Test that if we have corrupted an index segment it is rebuilt when the log is re-opened
   */
  @Test
  def testCorruptIndexRebuild() {
    // publish the messages and close the log
    val numMessages = 200
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 200: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)

    val config = LogConfig(logProps)
    var log = new Log(logDir, config, recoveryPoint = 0L, time.scheduler, time)
    for(i <- 0 until numMessages)
      log.append(TestUtils.singleMessageSet(TestUtils.randomBytes(10)))
    val indexFiles = log.logSegments.map(_.index.file)
    log.close()

    // corrupt all the index files
    for( file <- indexFiles) {
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write("  ")
      bw.close()
    }

    // reopen the log
    log = new Log(logDir, config, recoveryPoint = 200L, time.scheduler, time)
    assertEquals("Should have %d messages when log is reopened".format(numMessages), numMessages, log.logEndOffset)
    for(i <- 0 until numMessages)
      assertEquals(i, log.read(i, 100, None).messageSet.head.offset)
    log.close()
  }

  /**
   * Test the Log truncate operations
   */
  @Test
  def testTruncateTo() {
    val set = TestUtils.singleMessageSet("test".getBytes)
    val setSize = set.sizeInBytes
    val msgPerSeg = 10
    val segmentSize = msgPerSeg * setSize  // each segment will be 10 messages

    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, segmentSize: java.lang.Integer)

    // create a log
    val log = new Log(logDir, LogConfig(logProps), recoveryPoint = 0L, scheduler = time.scheduler, time = time)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)

    for (i<- 1 to msgPerSeg)
      log.append(set)

    assertEquals("There should be exactly 1 segments.", 1, log.numberOfSegments)
    assertEquals("Log end offset should be equal to number of messages", msgPerSeg, log.logEndOffset)

    val lastOffset = log.logEndOffset
    val size = log.size
    log.truncateTo(log.logEndOffset) // keep the entire log
    assertEquals("Should not change offset", lastOffset, log.logEndOffset)
    assertEquals("Should not change log size", size, log.size)
    log.truncateTo(log.logEndOffset + 1) // try to truncate beyond lastOffset
    assertEquals("Should not change offset but should log error", lastOffset, log.logEndOffset)
    assertEquals("Should not change log size", size, log.size)
    log.truncateTo(msgPerSeg/2) // truncate somewhere in between
    assertEquals("Should change offset", log.logEndOffset, msgPerSeg/2)
    assertTrue("Should change log size", log.size < size)
    log.truncateTo(0) // truncate the entire log
    assertEquals("Should change offset", 0, log.logEndOffset)
    assertEquals("Should change log size", 0, log.size)

    for (i<- 1 to msgPerSeg)
      log.append(set)

    assertEquals("Should be back to original offset", log.logEndOffset, lastOffset)
    assertEquals("Should be back to original size", log.size, size)
    log.truncateFullyAndStartAt(log.logEndOffset - (msgPerSeg - 1))
    assertEquals("Should change offset", log.logEndOffset, lastOffset - (msgPerSeg - 1))
    assertEquals("Should change log size", log.size, 0)

    for (i<- 1 to msgPerSeg)
      log.append(set)

    assertTrue("Should be ahead of to original offset", log.logEndOffset > msgPerSeg)
    assertEquals("log size should be same as before", size, log.size)
    log.truncateTo(0) // truncate before first start offset in the log
    assertEquals("Should change offset", 0, log.logEndOffset)
    assertEquals("Should change log size", log.size, 0)
  }

  /**
   * Verify that when we truncate a log the index of the last segment is resized to the max index size to allow more appends
   */
  @Test
  def testIndexResizingAtTruncation() {
    val set = TestUtils.singleMessageSet("test".getBytes)
    val setSize = set.sizeInBytes
    val msgPerSeg = 10
    val segmentSize = msgPerSeg * setSize  // each segment will be 10 messages
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, segmentSize: java.lang.Integer)
    val config = LogConfig(logProps)
    val log = new Log(logDir, config, recoveryPoint = 0L, scheduler = time.scheduler, time = time)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)
    for (i<- 1 to msgPerSeg)
      log.append(set)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)
    for (i<- 1 to msgPerSeg)
      log.append(set)
    assertEquals("There should be exactly 2 segment.", 2, log.numberOfSegments)
    assertEquals("The index of the first segment should be trimmed to empty", 0, log.logSegments.toList(0).index.maxEntries)
    log.truncateTo(0)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)
    assertEquals("The index of segment 1 should be resized to maxIndexSize", log.config.maxIndexSize/8, log.logSegments.toList(0).index.maxEntries)
    for (i<- 1 to msgPerSeg)
      log.append(set)
    assertEquals("There should be exactly 1 segment.", 1, log.numberOfSegments)
  }

  /**
   * When we open a log any index segments without an associated log segment should be deleted.
   */
  @Test
  def testBogusIndexSegmentsAreRemoved() {
    val bogusIndex1 = Log.indexFilename(logDir, 0)
    val bogusIndex2 = Log.indexFilename(logDir, 5)

    val set = TestUtils.singleMessageSet("test".getBytes)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, set.sizeInBytes * 5: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)
    val log = new Log(logDir,
                      LogConfig(logProps),
                      recoveryPoint = 0L,
                      time.scheduler,
                      time)

    assertTrue("The first index file should have been replaced with a larger file", bogusIndex1.length > 0)
    assertFalse("The second index file should have been deleted.", bogusIndex2.exists)

    // check that we can append to the log
    for(i <- 0 until 10)
      log.append(set)

    log.delete()
  }

  /**
   * Verify that truncation works correctly after re-opening the log
   */
  @Test
  def testReopenThenTruncate() {
    val set = TestUtils.singleMessageSet("test".getBytes)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, set.sizeInBytes * 5: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 10000: java.lang.Integer)
    val config = LogConfig(logProps)

    // create a log
    var log = new Log(logDir,
                      config,
                      recoveryPoint = 0L,
                      time.scheduler,
                      time)

    // add enough messages to roll over several segments then close and re-open and attempt to truncate
    for(i <- 0 until 100)
      log.append(set)
    log.close()
    log = new Log(logDir,
                  config,
                  recoveryPoint = 0L,
                  time.scheduler,
                  time)
    log.truncateTo(3)
    assertEquals("All but one segment should be deleted.", 1, log.numberOfSegments)
    assertEquals("Log end offset should be 3.", 3, log.logEndOffset)
  }

  /**
   * Test that deleted files are deleted after the appropriate time.
   */
  @Test
  def testAsyncDelete() {
    val set = TestUtils.singleMessageSet("test".getBytes)
    val asyncDeleteMs = 1000
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, set.sizeInBytes * 5: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 10000: java.lang.Integer)
    logProps.put(LogConfig.FileDeleteDelayMsProp, asyncDeleteMs: java.lang.Integer)
    val config = LogConfig(logProps)

    val log = new Log(logDir,
                      config,
                      recoveryPoint = 0L,
                      time.scheduler,
                      time)

    // append some messages to create some segments
    for(i <- 0 until 100)
      log.append(set)

    // files should be renamed
    val segments = log.logSegments.toArray
    val oldFiles = segments.map(_.log.file) ++ segments.map(_.index.file)
    log.deleteOldSegments((s) => true)

    assertEquals("Only one segment should remain.", 1, log.numberOfSegments)
    assertTrue("All log and index files should end in .deleted", segments.forall(_.log.file.getName.endsWith(Log.DeletedFileSuffix)) &&
                                                                 segments.forall(_.index.file.getName.endsWith(Log.DeletedFileSuffix)))
    assertTrue("The .deleted files should still be there.", segments.forall(_.log.file.exists) &&
                                                            segments.forall(_.index.file.exists))
    assertTrue("The original file should be gone.", oldFiles.forall(!_.exists))

    // when enough time passes the files should be deleted
    val deletedFiles = segments.map(_.log.file) ++ segments.map(_.index.file)
    time.sleep(asyncDeleteMs + 1)
    assertTrue("Files should all be gone.", deletedFiles.forall(!_.exists))
  }

  /**
   * Any files ending in .deleted should be removed when the log is re-opened.
   */
  @Test
  def testOpenDeletesObsoleteFiles() {
    val set = TestUtils.singleMessageSet("test".getBytes)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, set.sizeInBytes * 5: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    val config = LogConfig(logProps)
    var log = new Log(logDir,
                      config,
                      recoveryPoint = 0L,
                      time.scheduler,
                      time)

    // append some messages to create some segments
    for(i <- 0 until 100)
      log.append(set)

    log.deleteOldSegments((s) => true)
    log.close()

    log = new Log(logDir,
                  config,
                  recoveryPoint = 0L,
                  time.scheduler,
                  time)
    assertEquals("The deleted segments should be gone.", 1, log.numberOfSegments)
  }

  @Test
  def testAppendMessageWithNullPayload() {
    val log = new Log(logDir,
                      LogConfig(),
                      recoveryPoint = 0L,
                      time.scheduler,
                      time)
    log.append(new ByteBufferMessageSet(new Message(bytes = null)))
    val messageSet = log.read(0, 4096, None).messageSet
    assertEquals(0, messageSet.head.offset)
    assertTrue("Message payload should be null.", messageSet.head.message.isNull)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testAppendWithOutOfOrderOffsetsThrowsException() {
    val log = new Log(logDir,
      LogConfig(),
      recoveryPoint = 0L,
      time.scheduler,
      time)
    val messages = (0 until 2).map(id => new Message(id.toString.getBytes)).toArray
    messages.foreach(message => log.append(new ByteBufferMessageSet(message)))
    val invalidMessage = new ByteBufferMessageSet(new Message(1.toString.getBytes))
    log.append(invalidMessage, assignOffsets = false)
  }

  @Test
  def testCorruptLog() {
    // append some messages to create some segments
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)
    logProps.put(LogConfig.MaxMessageBytesProp, 64*1024: java.lang.Integer)
    val config = LogConfig(logProps)
    val set = TestUtils.singleMessageSet("test".getBytes)
    val recoveryPoint = 50L
    for(iteration <- 0 until 50) {
      // create a log and write some messages to it
      logDir.mkdirs()
      var log = new Log(logDir,
                        config,
                        recoveryPoint = 0L,
                        time.scheduler,
                        time)
      val numMessages = 50 + TestUtils.random.nextInt(50)
      for(i <- 0 until numMessages)
        log.append(set)
      val messages = log.logSegments.flatMap(_.log.iterator.toList)
      log.close()

      // corrupt index and log by appending random bytes
      TestUtils.appendNonsenseToFile(log.activeSegment.index.file, TestUtils.random.nextInt(1024) + 1)
      TestUtils.appendNonsenseToFile(log.activeSegment.log.file, TestUtils.random.nextInt(1024) + 1)

      // attempt recovery
      log = new Log(logDir, config, recoveryPoint, time.scheduler, time)
      assertEquals(numMessages, log.logEndOffset)
      assertEquals("Messages in the log after recovery should be the same.", messages, log.logSegments.flatMap(_.log.iterator.toList))
      Utils.delete(logDir)
    }
  }

  @Test
  def testCleanShutdownFile() {
    // append some messages to create some segments
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.MaxMessageBytesProp, 64*1024: java.lang.Integer)
    logProps.put(LogConfig.IndexIntervalBytesProp, 1: java.lang.Integer)
    val config = LogConfig(logProps)
    val set = TestUtils.singleMessageSet("test".getBytes)
    val parentLogDir = logDir.getParentFile
    assertTrue("Data directory %s must exist", parentLogDir.isDirectory)
    val cleanShutdownFile = new File(parentLogDir, Log.CleanShutdownFile)
    cleanShutdownFile.createNewFile()
    assertTrue(".kafka_cleanshutdown must exist", cleanShutdownFile.exists())
    var recoveryPoint = 0L
    // create a log and write some messages to it
    var log = new Log(logDir,
      config,
      recoveryPoint = 0L,
      time.scheduler,
      time)
    for(i <- 0 until 100)
      log.append(set)
    log.close()

    // check if recovery was attempted. Even if the recovery point is 0L, recovery should not be attempted as the
    // clean shutdown file exists.
    recoveryPoint = log.logEndOffset
    log = new Log(logDir, config, 0L, time.scheduler, time)
    assertEquals(recoveryPoint, log.logEndOffset)
    cleanShutdownFile.delete()
  }

  @Test
  def testParseTopicPartitionName() {
    val topic = "test_topic"
    val partition = "143"
    val dir = new File(logDir + topicPartitionName(topic, partition))
    val topicAndPartition = Log.parseTopicPartitionName(dir)
    assertEquals(topic, topicAndPartition.asTuple._1)
    assertEquals(partition.toInt, topicAndPartition.asTuple._2)
  }

  @Test
  def testParseTopicPartitionNameForEmptyName() {
    try {
      val dir = new File("")
      Log.parseTopicPartitionName(dir)
      fail("KafkaException should have been thrown for dir: " + dir.getCanonicalPath)
    } catch {
      case e: Exception => // its GOOD!
    }
  }

  @Test
  def testParseTopicPartitionNameForNull() {
    try {
      val dir: File = null
      Log.parseTopicPartitionName(dir)
      fail("KafkaException should have been thrown for dir: " + dir)
    } catch {
      case e: Exception => // its GOOD!
    }
  }

  @Test
  def testParseTopicPartitionNameForMissingSeparator() {
    val topic = "test_topic"
    val partition = "1999"
    val dir = new File(logDir + File.separator + topic + partition)
    try {
      Log.parseTopicPartitionName(dir)
      fail("KafkaException should have been thrown for dir: " + dir.getCanonicalPath)
    } catch {
      case e: Exception => // its GOOD!
    }
  }

  @Test
  def testParseTopicPartitionNameForMissingTopic() {
    val topic = ""
    val partition = "1999"
    val dir = new File(logDir + topicPartitionName(topic, partition))
    try {
      Log.parseTopicPartitionName(dir)
      fail("KafkaException should have been thrown for dir: " + dir.getCanonicalPath)
    } catch {
      case e: Exception => // its GOOD!
    }
  }

  @Test
  def testParseTopicPartitionNameForMissingPartition() {
    val topic = "test_topic"
    val partition = ""
    val dir = new File(logDir + topicPartitionName(topic, partition))
    try {
      Log.parseTopicPartitionName(dir)
      fail("KafkaException should have been thrown for dir: " + dir.getCanonicalPath)
    } catch {
      case e: Exception => // its GOOD!
    }
  }

  def topicPartitionName(topic: String, partition: String): String =
    File.separator + topic + "-" + partition

  @Test
  def testDeleteOldSegmentsMethod() {
    val set = TestUtils.singleMessageSet("test".getBytes)
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, set.sizeInBytes * 5: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    val config = LogConfig(logProps)
    val log = new Log(logDir,
      config,
      recoveryPoint = 0L,
      time.scheduler,
      time)

    // append some messages to create some segments
    for (i <- 0 until 100)
      log.append(set)

    log.deleteOldSegments(_ => true)
    assertEquals("The deleted segments should be gone.", 1, log.numberOfSegments)

    // append some messages to create some segments
    for (i <- 0 until 100)
      log.append(set)

    log.delete()
    assertEquals("The number of segments should be 0", 0, log.numberOfSegments)
    assertEquals("The number of deleted segments shoud be zero.", 0, log.deleteOldSegments(_ => true))
  }
}
