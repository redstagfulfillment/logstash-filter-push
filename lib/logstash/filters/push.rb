# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "logstash/environment"
require "set"
#
# This filter will collapse parts of multiple messages with the same unique identifier into a single Logstash event
# with the specified fields in an array.
#
# The config looks like this:
#
#     filter {
#       push {
#         unique_field => "WONumber"
#         target => "order_items"
#         fields => ["TxnLine Quantity","TxnLine Item"]
#       }
#     }
#
class LogStash::Filters::Push < LogStash::Filters::Base

  config_name "push"

  config :unique_field, :validate => :string, :required => true

  config :target, :validate => :string, :required => true

  config :fields, :validate => :array, :required => true

  # The stream identity is how the multiline filter determines which stream an
  # event belongs to. This is generally used for differentiating, say, events
  # coming from multiple files in the same file input, or multiple connections
  # coming from a tcp input.
  #
  # The default value here is usually what you want, but there are some cases
  # where you want to change it. One such example is if you are using a tcp
  # input with only one client connecting at any time. If that client
  # reconnects (due to error or client restart), then logstash will identify
  # the new connection as a new stream and break any multiline goodness that
  # may have occurred between the old and new connection. To solve this use
  # case, you can use "%{@source_host}.%{@type}" instead.
  config :stream_identity , :validate => :string, :default => "%{host}.%{path}.%{type}"

  public
  def initialize(config = {})
    super
    @threadsafe = false
    @pending = Hash.new
  end # def initialize

  public
  def register
    @logger.debug("Registered push plugin", :type => @type, :config => @config)
  end # def register

  public
  def filter(event)
    return unless filter?(event)

    event.tag("push")
    key = event.sprintf(@stream_identity)
    pending = @pending[key]

    match = pending && (pending[@unique_field] == event[@unique_field] || event[@unique_field].nil?)
    @logger.debug? && @logger.debug("Push", :unique_field => @unique_field, @unique_field => event[@unique_field], :match => match)

    subdocument = event.to_hash_with_metadata.select {|key, value| @fields.include?(key) }

    if match
      # previous event is part of this event.
      pending[@target] << subdocument
      event.cancel
    else
      event[@target] = [subdocument]
      # this event is not part of the previous event
      # if we have a pending event, it's done, send it.
      # put the current event into pending
      if pending
        tmp = event.to_hash
        event.overwrite(pending)
        @pending[key] = LogStash::Event.new(tmp)
      else
        @pending[key] = event
        event.cancel
      end # if/else pending
    end # if/else match
  end # def filter

  # flush any pending messages
  # called at regular interval without options and at pipeline shutdown with the :final => true option
  # @param options [Hash]
  # @option options [Boolean] :final => true to signal a final shutdown flush
  # @return [Array<LogStash::Event>] list of flushed events
  public
  def flush(options = {})
    # note that thread safety concerns are not necessary here because the multiline filter
    # is not thread safe thus cannot be run in multiple filterworker threads and flushing
    # is called by the same thread

    if options[:final]
      @pending.values.each do |event|
      event.uncancel
    end
    else
      []
    end
  end # def flush

  public
  def teardown
    # nothing to do
  end

end # class LogStash::Filters::Multiline