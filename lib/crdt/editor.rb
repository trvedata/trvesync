require 'dispel'

module CRDT
  # A basic curses-based interactive text editor
  class Editor
    attr_reader :peer

    def initialize(peer, options={})
      @peer = peer or raise ArgumentError, 'peer must be set'
      @options = options
      @canvas_size = [0, 0]
      @cursor_id = nil
    end

    # Draw app and redraw after each keystroke
    def run
      Dispel::Screen.open(@options) do |screen|
        resize(screen.columns, screen.lines)
        render(screen)

        Dispel::Keyboard.output do |key|
          break if key_pressed(key, screen) == :quit
        end
      end
    end

    def resize(columns, lines)
      @canvas_size = [columns, lines]
    end

    def render(screen)
      screen.debug_key(@last_key) if @last_key && @options[:debug_keys]
      resize(screen.columns, screen.lines) if @last_key == :resize

      @lines = ['']
      @item_ids = [[]]
      @cursor = [0, 0]
      word_boundary = 0

      @peer.ordered_list.each_item do |item|
        if item.insert_id == @cursor_id
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

              if @item_ids.last.include? @cursor_id
                @cursor = [@item_ids.last.index(@cursor_id), @item_ids.size - 1]
              end
            end
          end
        end
      end

      @item_ids.last << nil # insertion point for appending at the end
      @cursor = [@item_ids.last.size - 1, @item_ids.size - 1] if @cursor_id.nil?
      @lines << '' while @lines.size < @canvas_size[1]
      screen.draw(@lines.join("\n"), [], @cursor.reverse)
    end

    def key_pressed(key, screen)
      @last_key = key
      modified = false

      case key
      when :"Ctrl+q", :"Ctrl+c" then return :quit

      # moving cursor
      when :left, :right, :up, :down then move_cursor key
      when :page_up                  then move_cursor :page_up
      when :page_down                then move_cursor :page_down
      when :"Ctrl+left",  :"Alt+b"   then move_cursor :word_left
      when :"Ctrl+right", :"Alt+f"   then move_cursor :word_right
      when :home, :"Ctrl+a"          then move_cursor :line_begin
      when :end,  :"Ctrl+e"          then move_cursor :line_end

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

    def move_cursor(command)
      case command
      when :left
        if @cursor[0] > 0
          @cursor = [@cursor[0] - 1, @cursor[1]]
        elsif @cursor[1] > 0
          @cursor[1] -= 1
          @cursor[0] = end_of_line
        end
        @cursor_x = @cursor[0]

      when :right
        if @cursor[0] < end_of_line
          @cursor = [@cursor[0] + 1, @cursor[1]]
        elsif @cursor[1] < @item_ids.size - 1
          @cursor = [0, @cursor[1] + 1]
        end
        @cursor_x = @cursor[0]

      when :up
        if @cursor[1] > 0
          @cursor[1] -= 1
          @cursor[0] = [[@cursor[0], @cursor_x].max, end_of_line].min
        else
          @cursor[0] = @cursor_x = 0
        end

      when :down
        if @cursor[1] < @item_ids.size - 1
          @cursor[1] += 1
          @cursor[0] = [[@cursor[0], @cursor_x].max, end_of_line].min
        else
          @cursor[0] = @cursor_x = end_of_line
        end

      when :line_begin
        @cursor[0] = @cursor_x = 0

      when :line_end
        @cursor[0] = end_of_line
        @cursor_x = @canvas_size[0]
      end

      @cursor_id = @item_ids[@cursor[1]][@cursor[0]]
    end

    def end_of_line
      @item_ids[@cursor[1]].size - 1
    end

    # Insert a character at the current cursor position
    def insert(char)
      @peer.ordered_list.insert_before_id(@cursor_id, char)
      :insert
    end

    # Delete characters before (negative) or after (positive) the current cursor position
    def delete(num_chars)
      if num_chars < 0
        @peer.ordered_list.delete_before_id(@cursor_id, -num_chars)
      elsif @cursor_id
        @cursor_id = @peer.ordered_list.delete_after_id(@cursor_id, num_chars)
      end
      :delete
    end
  end
end
