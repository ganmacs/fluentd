require_relative '../helper'
require 'fluent/plugin/token_bucket'

class TokenBucketTest < Test::Unit::TestCase
  class TestBlockTimer < Fluent::TokenBucket::BlockTimer
    def initialize
      super
      @_blocking = false
    end

    def tick
      @_blocking = false
      @mutex.synchronize { @cond.broadcast }
      false
    end

    def block(time)
      @_blocking = true
      super
    ensure
      @_blocking = false
    end

    def wait!
      until @_blocking
        sleep 0.1
      end
    end
  end

  # wait for starting given block
  def invoke_on_thread
    q = Queue.new
    thread = Thread.new {
      q.push(1)
      yield
    }
    q.pop

    thread
  end

  sub_test_case '#start' do
    test 'gives tokens to some value' do
      tb = Fluent::TokenBucket.new(10, $log)
      assert_true tb.full?

      thread = invoke_on_thread { tb.start }

      assert_false tb.full?
      assert_equal 2, tb.capacity
    ensure
      tb.stop
      thread.join
    end

    test 'gives tokens to some value per tick' do
      bt = TestBlockTimer.new
      stub(Fluent::TokenBucket::BlockTimer).new { bt }
      tb = Fluent::TokenBucket.new(10, $log)

      thread = invoke_on_thread { tb.start }

      bt.wait!
      assert_equal 2, tb.capacity

      # token_window is 2,2,2,2,1,1,2,2,2,2,1,1
      [4, 6, 8, 9, 10, 12, 14, 16, 18, 19, 20].each do |v|
        bt.tick
        bt.wait!
        assert_equal v, tb.capacity
      end
    ensure
      tb.stop
      thread.join
    end

    test 'tokens has a limit' do
      bt = TestBlockTimer.new
      stub(Fluent::TokenBucket::BlockTimer).new { bt }
      tb = Fluent::TokenBucket.new(10, $log, 1) # limit is 10*1 = 10

      thread = invoke_on_thread { tb.start }

      bt.wait!
      assert_equal 2, tb.capacity

      [4, 6, 8, 9, 10, 10, 10, 10].each do |v|
        bt.tick
        bt.wait!
        assert_equal v, tb.capacity
      end
    ensure
      tb.stop
      thread.join
    end

    sub_test_case '#add' do
      test 'consume the token' do
        bt = TestBlockTimer.new
        stub(Fluent::TokenBucket::BlockTimer).new { bt }
        tb = Fluent::TokenBucket.new(10, $log)

        thread = invoke_on_thread { tb.start }

        bt.wait!
        assert_equal 2, tb.capacity

        tb.add(2)
        assert_equal 0, tb.capacity
      ensure
        tb.stop
        thread.join
      end

      test 'token cannot be negative' do
        bt = TestBlockTimer.new
        stub(Fluent::TokenBucket::BlockTimer).new { bt }
        tb = Fluent::TokenBucket.new(10, $log)

        thread = invoke_on_thread { tb.start }

        bt.wait!
        assert_equal 2, tb.capacity

        tb.add(100)
        assert_equal 0, tb.capacity

        tb.add(1)
        assert_equal 0, tb.capacity

        tb.add(1000)
        assert_equal 0, tb.capacity
      ensure
        tb.stop
        thread.join
      end
    end

    sub_test_case '#full?' do
      test 'return true if tokens is zero' do
        bt = TestBlockTimer.new
        stub(Fluent::TokenBucket::BlockTimer).new { bt }
        tb = Fluent::TokenBucket.new(10, $log)
        assert_true tb.full?

        thread = invoke_on_thread { tb.start }

        assert_equal 2, tb.capacity
        assert_false tb.full?

        tb.add(1)
        assert_equal 1, tb.capacity
        assert_false tb.full?

        tb.add(1)
        assert_equal 0, tb.capacity
        assert_true tb.full?

        tb.add(1)
        assert_equal 0, tb.capacity
        assert_true tb.full?

        bt.tick
        bt.wait!

        tb.add(1)
        assert_equal 1, tb.capacity
        assert_false tb.full?

        tb.add(1)
        assert_equal 0, tb.capacity
        assert_true tb.full?

        tb.add(1)
        assert_equal 0, tb.capacity
        assert_true tb.full?
      ensure
        tb.stop
        thread.join
      end
    end
  end
end
