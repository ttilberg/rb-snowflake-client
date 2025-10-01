# frozen_string_literal: true

require "concurrent"

require_relative "result"

module RubySnowflake
  class StreamingResult < Result
    def initialize(partition_count, row_type_data, retreive_proc)
      super(partition_count, row_type_data)
      @retreive_proc = retreive_proc
    end

    def each
      return to_enum(:each) unless block_given?

      thread_pool = Concurrent::FixedThreadPool.new 1

      RubySnowflake::Client::DEFAULT_LOGGER.debug { "StreamingResult#each: data.size: #{data.size}" }

      # data is a Concurrent::Array, each entry represents a partition of data to load
      data.each_with_index do |_partition, index|
        # The first response from Snowflake includes the first partition of data.
        # This means `data[0]` is prefilled from the initial response, and `data[next_index]` will not be nil.
        next_index = [index+1, data.size-1].min
        if data[next_index].nil? # prefetch
          data[next_index] = Concurrent::Future.execute(executor: thread_pool) do
            RubySnowflake::Client::DEFAULT_LOGGER.debug { "Preparing partition #{next_index}" }
            @retreive_proc.call(next_index)
          end
        end

        if data[index].is_a? Concurrent::Future
          RubySnowflake::Client::DEFAULT_LOGGER.debug { "Awaiting partition #{index}" }
          data[index] = data[index].value.tap do # wait for it to finish
            RubySnowflake::Client::DEFAULT_LOGGER.debug { "Received partition #{index}" }
          end
        end

        data[index].each do |row|
          yield wrap_row(row)
        end
        # TODO: I think this equates to a memory leak with regards to "streaming".
        # It seems that as we progress `index`, previous `data[0..index-1]` are not cleared.
        # Maybe this is intended so you can iterate the result object multiple times,
        # but that seems like it goes against the point of the streaming interface.
        #
        # TODO ?: data[index].clear
      end
    end


    def size
      not_implemented
    end

    def last
      not_implemented
    end

    private
      def not_implemented
        raise "not implemented on streaming result set"
      end
  end
end
