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

    # Draw app and redraw after each keystroke (or paste)
    def run
      Dispel::Screen.open(@options) do |screen|
        resize(screen.columns, screen.lines)
        render(screen)

        Dispel::Keyboard.output do |key|
          screen.debug_key(key) if @options[:debug_keys]
          if key == :resize
            resize(screen.columns, screen.lines)
          else
            result = key_pressed(key)
            break if result == :quit
          end

          render(screen)
        end
      end
    end

    private

    def render(screen)
      @lines = ['']
      @item_ids = [[]]
      @cursor = [0, 0]
      word_boundary = 0

      @peer.ordered_list.each_item do |item|
        if item.insert_id == @cursor_id
          @cursor = [@lines.size - 1, @lines.last.size]
        end

        if item.delete_ts.nil?
          if item.value == "\n"
            @lines << ''
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
            end
          end
        end
      end

      @cursor = [@lines.size - 1, @lines.last.size] if @cursor_id.nil?
      screen.draw(@lines.join("\n"), [], @cursor)
    end

    def key_pressed(key)
      case key
      when :"Ctrl+w" then :quit
      when :"Ctrl+q" then :quit
      when :"Ctrl+c" then :quit

      # moving cursor
      when :left                   then move_cursor :relative, -1,  0
      when :right                  then move_cursor :relative,  1,  0
      when :up                     then move_cursor :relative,  0, -1
      when :down                   then move_cursor :relative,  0,  1
      when :page_up                then move_cursor :page_up
      when :page_down              then move_cursor :page_down
      when :"Ctrl+left",  :"Alt+b" then move_cursor :word_left
      when :"Ctrl+right", :"Alt+f" then move_cursor :word_right
      when :home, :"Ctrl+a"        then move_cursor :line_begin
      when :end,  :"Ctrl+e"        then move_cursor :line_end

      # editing text
      when :tab       then insert("\t")
      when :enter     then insert("\n")
      when :backspace then delete(-1)
      when :delete    then delete(1)
      else
        insert(key) if key.is_a?(String) && key.size == 1
      end
    end

    def move_cursor(command, horizontal, vertical)
      # TODO
    end

    # Insert a character at the current cursor position
    def insert(char)
      @peer.ordered_list.insert_before_id(@cursor_id, char)
    end

    def delete(num_chars)
      # TODO
    end

    def resize(columns, lines)
      @canvas_size = [columns, lines]
    end
  end
end
