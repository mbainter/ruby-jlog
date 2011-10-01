#!/usr/bin/ruby

require 'test/unit'
require 'jlog'
# require 'pp'

class TC_JLog < Test::Unit::TestCase
   def test_CreateJLog
      assert_nothing_raised {@jo = JLog.new("/tmp/junit.log")}
      assert_kind_of(JLog, @jo, "JLog Object creation failed");
      assert_nothing_raised {@jo.close}
   end

   def test_AddSubscriber
      @jo = JLog.new("/tmp/junit.log")
      assert_nothing_raised do
         @jo.add_subscriber("TestSub")
         @jo.add_subscriber("TestSubRemove")

      end

      assert_nothing_raised do
         @flag = 2;
         @jo.list_subscribers.each { |s|
            if(s =~ /TestSub(Remove|)/)
               @flag -= 1;
            end
         }
      end
      assert_equal(0, @flag, "The two expected subscribers exist")
      @jo.close
   end

   def test_RMSubscriber
      @jo = JLog.new("/tmp/junit.log")
      assert_nothing_raised { @jo.remove_subscriber("TestSubRemove") }
      @jo.close
      @jo = JLog.new("/tmp/junit.log")
      @jo.list_subscribers.each { |s|
         assert_not_equal("TestSubRemove", s, "Test Subscriber was not removed")
      }
      @jo.close
   end
end

class TC_JLogWriter < Test::Unit::TestCase
   def test_Open
      assert_nothing_raised {@jwo = JLog::Writer.new("/tmp/junit.log")}
      assert_kind_of(JLog::Writer, @jwo, "JLogWriter Object creation failed");
      assert_nothing_raised { @jwo.open }
      assert_nothing_raised {@jwo.close}
   end

   def test_Write
      @jwo = JLog::Writer.new("/tmp/junit.log")
      assert_nothing_raised do
         @jwo.write("Test Unit") 
         1.upto(10) do |n| 
            @jwo.write("Test Unit #{n}")
         end
      end
      @jwo.close
   end
end

class TC_JLogReader < Test::Unit::TestCase
   def test_Open
      assert_nothing_raised {@jro = JLog::Reader.new("/tmp/junit.log")}
      assert_kind_of(JLog::Reader, @jro, "JLogReader Object creation failed");
      assert_nothing_raised {@jro.open("TestSub")}
      assert_nothing_raised {@jro.close}
   end

   def test_Open
      @jro = JLog::Reader.new("/tmp/junit.log")
      assert_nothing_raised { @jro.open("TestSub") }
      @jro.close
   end

#These tests are oddly broken
=begin
   def test_Read_Once
      @jro = JLog::Reader.new("/tmp/junit.log")
      @jro.open("TestSub")
      assert_equal("Test Unit", @jro.read, "First Log Message does not match")
      assert_equal("Test Unit", @jro.read, "LogMessage was inappropriately CheckPointed!")
   #   @jro.close
   end
=end

   def test_Rewind
      @jro = JLog::Reader.new("/tmp/junit.log")
      @jro.open("TestSub")
      @res1 = @jro.read
      assert_nothing_raised { @jro.rewind }
      @res2 = @jro.read
      assert_equal(@res1, @res2, "Rewound Log Messages do not match")
   #   @jro.close
   end
   
=begin
   def test_Checkpoint
      @jro = JLog::Reader.new("/tmp/junit.log")
      @jro.open("TestSub")
      res1 = @jro.read
      assert_nothing_raised { @jro.checkpoint }
      res2 = @jro.read
      assert_not_equal(res1, res2, "Checkpointed Log Messages should not match")
      @jro.checkpoint
      @jro.close
   end
=end

   def test_AutoCheckpoint
      @jro = JLog::Reader.new("/tmp/junit.log")
      @jro.open("TestSub")
      assert_equal(1, @jro.auto_checkpoint(1), "AutoCheckpoint not successfully set");
      @lastmsg = nil
      while @msg = @jro.read do
         assert_not_equal(@msg, @lastmsg, "Auto_Checkpointed Log Messages should not match")
         @lastmsg = @msg
      end
      @jro.close
   end
end
