#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

module Fluent
  # https://en.wikipedia.org/wiki/Token_bucket
  class TokenBucket
    class BlockTimer
      def initialize
        @mutex = Mutex.new
        @cond = ConditionVariable.new
        @finish = false
      end

      def finish
        @finish = true
        @mutex.synchronize { @cond.broadcast }
      end

      def block(time)
        if @finish
          return true
        end

        @mutex.synchronize {
          @cond.wait(@mutex, time)
        }
        @finish
      end
    end

    TOKEN_WINDON_COUNT = 6
    TOKEN_WINDON = 60 / TOKEN_WINDON_COUNT

    def initialize(token_per_interval, log, limit = 10)
      @token_per_interval = token_per_interval
      @log = log
      @stop = false
      @tokens = 0
      @max_tokens = limit * token_per_interval
      @blocker = BlockTimer.new
      @mutex = Mutex.new
    end

    def add(val)
      @mutex.synchronize do
        return if @tokens <= 0

        if @tokens < val
          @tokens = 0
        else
          @tokens -= val
        end
      end
    end

    def full?
      @tokens <= 0
    end

    def capacity
      @tokens
    end

    # blocking
    def start
      @log.debug('bucket started')
      loop do
        token_windows.each do |token|
          @mutex.synchronize do
            t = @tokens + token
            if t > @max_tokens
              @tokens = @max_tokens
            else
              @tokens = t
            end
          end

          @log.debug("added token: #{token}, total token #{@token}")

          if (@stop = @blocker.block(TOKEN_WINDON))
            @log.debug('bucket finished')
            break
          end
        end

        if @stop
          break
        end
      end
    end

    def stop
      @blocker.finish
    end

    private

    # create token volume per TOKEN_WINDOW
    def token_windows
      @token_windows ||=
        begin
          ary = Array.new(TOKEN_WINDON_COUNT, @token_per_interval / TOKEN_WINDON_COUNT)
          (@token_per_interval % TOKEN_WINDON_COUNT).times { |v| ary[v] += 1 }
          ary
        end
    end
  end
end
