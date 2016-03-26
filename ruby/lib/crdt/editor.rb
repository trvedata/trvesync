require 'dispel'
require 'crdt/network'

module CRDT
  # A basic curses-based interactive text editor
  class Editor
    attr_reader :peer

    def initialize(peer, options={})
      @peer = peer or raise ArgumentError, 'peer must be set'
      @options = options
      @logger = options[:logger] || lambda {|msg| }
      @network = CRDT::Network.new(peer, options[:websocket], @logger)
      @canvas_size = [0, 0]
      @scroll_lines = 0
    end

    # Draw app and redraw after each keystroke
    def run
      @network.run
      wait_for_initial_sync

      Dispel::Screen.open(@options) do |screen|
        resize(screen.columns, screen.lines)
        render(screen)

        Dispel::Keyboard.output(timeout: 0.1) do |key|
          @network.poll
          break if key_pressed(key, screen) == :quit
        end
      end
    ensure
      save
    end

    # If this editor instance was started without specifying a channel ID, that means it is the one
    # to create the channel and register the schema. In that case we don't need to wait. However, if
    # we are joining an existing channel, we need to wait until we have at least received the schema
    # information from the channel before we can start up the editor.
    def wait_for_initial_sync
      return if @peer.default_schema_id

      puts 'Connecting to server...'
      while @peer.default_schema_id.nil?
        sleep 0.1
        @network.poll
      end
    end

    def resize(columns, lines)
      @canvas_size = [columns, lines]
    end

    def render(screen)
      resize(screen.columns, screen.lines) if @last_key == :resize

      @lines = ['']
      @item_ids = [[]]
      @cursor = [0, 0]
      word_boundary = 0

      @peer.ordered_list.each_item do |item|
        if @peer.cursor_id == item.insert_id
          @cursor = [@item_ids.last.size, @item_ids.size - 1]
        end

        if item.delete_ts.nil?
          if item.value == "\n"
            @lines << ''
            @item_ids.last << item.insert_id
            @item_ids << []
            word_boundary = 0
          else
            @lines.last << item.value
            @item_ids.last << item.insert_id
            word_boundary = @lines.last.size if [" ", "\t", "-"].include? item.value

            if @lines.last.size != @item_ids.last.size
              raise "Document contains item that is not a single character: #{item.value.inspect}"
            end

            if @lines.last.size >= @canvas_size[0]
              if word_boundary == 0 || word_boundary == @lines.last.size
                @lines << ''
                @item_ids << []
              else
                word_len = @lines.last.size - word_boundary
                @lines    << @lines.   last.slice!(word_boundary, word_len) || ''
                @item_ids << @item_ids.last.slice!(word_boundary, word_len) || []
              end
              word_boundary = 0

              if @item_ids.last.include? @peer.cursor_id
                @cursor = [@item_ids.last.index(@peer.cursor_id), @item_ids.size - 1]
              end
            end
          end
        end
      end

      @item_ids.last << nil # insertion point for appending at the end
      @cursor = [@item_ids.last.size - 1, @item_ids.size - 1] if @peer.cursor_id.nil?

      @scroll_lines = [@scroll_lines, @cursor[1]].min
      if @cursor[1] - @scroll_lines >= @canvas_size[1] - 1
        @scroll_lines = @cursor[1] - (@canvas_size[1] - 1) + 1
      end

      viewport = @lines.slice(@scroll_lines, @canvas_size[1] - 1)
      viewport << '' while viewport.size < @canvas_size[1] - 1
      viewport << "Channel: #{@peer.channel_id}"[0...@canvas_size[0]]
      screen.draw(viewport.join("\n"), [], [@cursor[1] - @scroll_lines, @cursor[0]])
      screen.debug_key(@last_key) if @last_key && @options[:debug_keys]
    end

    def key_pressed(key, screen)
      @last_key = key
      modified = false

      case key
      when :"Ctrl+q", :"Ctrl+c" then return :quit

      # moving cursor
      when :left                     then move_cursor_left
      when :right                    then move_cursor_right
      when :up                       then move_cursor_up
      when :down                     then move_cursor_down
      when :page_up                  then move_cursor_page_up
      when :page_down                then move_cursor_page_down
      when :"Ctrl+left",  :"Alt+b"   then move_cursor_word_left
      when :"Ctrl+right", :"Alt+f"   then move_cursor_word_right
      when :home, :"Ctrl+a"          then move_cursor_line_begin
      when :end,  :"Ctrl+e"          then move_cursor_line_end

      # editing text
      when :enter     then modified = true; insert("\n")
      when :backspace then modified = true; delete(-1)
      when :delete    then modified = true; delete(1)
      else
        if key.is_a?(String) && key.size == 1
          insert(key)
          modified = true
        end
      end

      render(screen)
      @cursor_x = @cursor[0] if modified
    end

    private

    def move_cursor_left
      if @cursor[0] > 0
        @cursor = [@cursor[0] - 1, @cursor[1]]
      elsif @cursor[1] > 0
        @cursor[1] -= 1
        @cursor[0] = end_of_line
      end

      @cursor_x = @cursor[0]
      @peer.cursor_id = @item_ids[@cursor[1]][@cursor[0]]
    end

    def move_cursor_right
      if @cursor[0] < end_of_line
        @cursor = [@cursor[0] + 1, @cursor[1]]
      elsif @cursor[1] < @item_ids.size - 1
        @cursor = [0, @cursor[1] + 1]
      end

      @cursor_x = @cursor[0]
      @peer.cursor_id = @item_ids[@cursor[1]][@cursor[0]]
    end

    def move_cursor_up
      if @cursor[1] > 0
        @cursor[1] -= 1
        @cursor[0] = [[@cursor[0], @cursor_x || 0].max, end_of_line].min
      else
        @cursor[0] = @cursor_x = 0
      end

      @peer.cursor_id = @item_ids[@cursor[1]][@cursor[0]]
    end

    def move_cursor_down
      if @cursor[1] < @item_ids.size - 1
        @cursor[1] += 1
        @cursor[0] = [[@cursor[0], @cursor_x || 0].max, end_of_line].min
      else
        @cursor[0] = @cursor_x = end_of_line
      end

      @peer.cursor_id = @item_ids[@cursor[1]][@cursor[0]]
    end

    def move_cursor_page_up
      # TODO
    end

    def move_cursor_page_down
      # TODO
    end

    def move_cursor_word_left
      seen_word_char = false
      last_item_id = @peer.cursor_id
      is_first_item = true

      @peer.ordered_list.each_item(@peer.cursor_id, :backwards) do |item|
        next if item.delete_ts
        if is_first_item
          is_first_item = false
          next
        end

        if item.value =~ /[[:word:]]/
          seen_word_char = true
        elsif seen_word_char
          @peer.cursor_id = last_item_id
          return
        end

        last_item_id = item.insert_id
      end

      @peer.cursor_id = last_item_id
    end

    def move_cursor_word_right
      return if @peer.cursor_id.nil?
      seen_word_char = false

      @peer.ordered_list.each_item(@peer.cursor_id, :forwards) do |item|
        next if item.delete_ts

        if item.value =~ /[[:word:]]/
          seen_word_char = true
        elsif seen_word_char
          @peer.cursor_id = item.insert_id
          return
        end
      end

      @peer.cursor_id = nil
    end

    def move_cursor_line_begin
      @cursor[0] = @cursor_x = 0
      @peer.cursor_id = @item_ids[@cursor[1]][@cursor[0]]
    end

    def move_cursor_line_end
      @cursor[0] = end_of_line
      @cursor_x = @canvas_size[0]
      @peer.cursor_id = @item_ids[@cursor[1]][@cursor[0]]
    end

    def end_of_line
      @item_ids[@cursor[1]].size - 1
    end

    # Insert a character at the current cursor position
    def insert(char)
      @peer.ordered_list.insert_before_id(@peer.cursor_id, char)
      :insert
    end

    # Delete characters before (negative) or after (positive) the current cursor position
    def delete(num_chars)
      if num_chars < 0
        @peer.ordered_list.delete_before_id(@peer.cursor_id, -num_chars)
      elsif @peer.cursor_id
        @peer.cursor_id = @peer.ordered_list.delete_after_id(@peer.cursor_id, num_chars)
      end
      :delete
    end

    # Saves the state of the peer to the filename given in the options. To avoid corruption in the
    # case of an inopportune crash or power failure, first writes the data to a temporary file, then
    # renames it to the correct filename.
    def save
      return if @options[:filename].nil?
      filename = File.absolute_path(@options[:filename])
      temp_fn = File.join(File.dirname(filename), '.' + File.basename(filename) + ".tmp.#{Process.pid}")
      File.open(temp_fn, 'wb') {|file| @peer.save(file) }
      File.rename(temp_fn, filename)
    end
  end
end
