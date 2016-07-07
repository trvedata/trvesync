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

      if @options[:passive]
        loop do
          @network.poll
          sleep 0.1
        end
      else
        Dispel::Screen.open(@options) do |screen|
          resize(screen.columns, screen.lines)
          render(screen)

          Dispel::Keyboard.output(timeout: 0.1) do |key|
            @network.poll
            break if key_pressed(key, screen) == :quit
          end
        end
      end
    ensure
      puts 'Saving file...'
      begin
        save
      rescue => e
        puts "Error while saving: #{$!}"
        puts e.backtrace.map{|line| "\t#{line}\n" }.join
      end
      puts "To join this document, use the following options:"
      puts "    -j #{peer.channel_id} \\"
      puts "    -k #{peer.secret_key}"
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

    class Paragraph < Struct.new(:lines, :item_ids, :cursor, :ends_with_newline, :start_id, :next_para_id)
      def num_lines
        lines.size
      end
    end

    def render_paragraph(start_id)
      lines = ['']
      item_ids = [[]]
      cursor = nil
      word_boundary = 0
      newline_found = false

      @peer.ordered_list.each_item(start_id, :forwards) do |item|
        start_id ||= item.insert_id
        return Paragraph.new(lines, item_ids, cursor, newline_found, start_id, item.insert_id) if newline_found
        cursor = [item_ids.last.size, item_ids.size - 1] if @peer.cursor_id == item.insert_id

        if item.delete_ts.nil?
          if item.value == "\n"
            item_ids.last << item.insert_id
            newline_found = true
          else
            lines.last << item.value
            item_ids.last << item.insert_id
            word_boundary = lines.last.size if [" ", "\t", "-"].include? item.value

            if lines.last.size != item_ids.last.size
              raise "Document contains item that is not a single character: #{item.value.inspect}"
            end

            if lines.last.size >= @canvas_size[0]
              if word_boundary == 0 || word_boundary == lines.last.size
                lines << ''
                item_ids << []
              else
                word_len = lines.last.size - word_boundary
                lines    << lines.   last.slice!(word_boundary, word_len) || ''
                item_ids << item_ids.last.slice!(word_boundary, word_len) || []
              end
              word_boundary = 0

              if cursor && item_ids.last.include?(@peer.cursor_id)
                cursor = [item_ids.last.index(@peer.cursor_id), item_ids.size - 1]
              end
            end
          end
        end
      end

      item_ids.last << nil unless newline_found # insertion point for appending at the end
      Paragraph.new(lines, item_ids, cursor, newline_found, start_id, nil)
    end

    def render(screen)
      resize(screen.columns, screen.lines) if @last_key == :resize

      start_id = @paragraphs && @paragraphs.first && @paragraphs.first.start_id
      @paragraphs = []
      @cursor = nil
      num_lines = 0

      loop do
        para = render_paragraph(start_id)
        @paragraphs << para
        @cursor = [para.cursor[0], para.cursor[1] + num_lines] if para.cursor
        num_lines += para.num_lines
        start_id = para.next_para_id
        break if start_id.nil? || (@cursor && num_lines - @scroll_lines >= @canvas_size[1] - 1)
      end

      if start_id.nil? && @paragraphs.last.ends_with_newline
        @paragraphs << Paragraph.new([''], [[nil]], nil, false, nil, nil)
        num_lines += 1
      end

      @cursor ||= [@paragraphs.last.item_ids.last.size - 1, num_lines - 1]
      @scroll_lines = [@scroll_lines, @cursor[1]].min
      @scroll_lines = [@scroll_lines, @cursor[1] - @canvas_size[1] + 2].max

      shift_paragraph while @scroll_lines >= @paragraphs.first.num_lines

      viewport = @paragraphs.flat_map{|para| para.lines }.slice(@scroll_lines, @canvas_size[1] - 1)
      viewport << '' while viewport.size < @canvas_size[1] - 1
      viewport << "Channel: #{@peer.channel_id}"[0...@canvas_size[0]]
      screen.draw(viewport.join("\n"), [], [@cursor[1] - @scroll_lines, @cursor[0]])
      screen.debug_key(@last_key) if @last_key && @options[:debug_keys]
    end

    # Drop the first paragraph from the range of paragraphs to be rendered (because we scrolled down)
    def shift_paragraph
      @scroll_lines -= @paragraphs.first.num_lines
      @cursor[1] -= @paragraphs.first.num_lines
      @paragraphs.shift
    end

    # Add a preceding paragraph to the range of paragraphs to be rendered (because we scrolled up).
    # Returns true if a paragraph was added, and false if we're at the beginning of the document.
    def unshift_paragraph
      last_id = nil
      newlines = 0
      @peer.ordered_list.each_item(@paragraphs.first.start_id, :backwards) do |item|
        newlines += 1 if item.delete_ts.nil? && item.value == "\n"
        break if newlines == 2
        last_id = item.insert_id
      end
      return false if newlines == 0

      para = render_paragraph(last_id)
      @scroll_lines += para.num_lines
      @cursor[1] += para.num_lines
      @paragraphs.unshift(para)
      true
    end

    # Adds a paragraph to the end of the range of paragraphs to be rendered (because we scrolled
    # down). Returns true if a paragraph was added, and false if we're at the end of the document.
    def push_paragraph
      return false if @paragraphs.last.next_para_id.nil?
      para = render_paragraph(@paragraphs.last.next_para_id)
      if para.item_ids == [[nil]]
        false
      else
        @paragraphs << para
        true
      end
    end

    def key_pressed(key, screen)
      @last_key = key
      modified = false

      case key
      when :"Ctrl+q", :"Ctrl+c" then return :quit
      when :"Ctrl+s"            then save

      # moving cursor
      when :left                     then move_cursor_left
      when :right                    then move_cursor_right
      when :up                       then move_cursor_up
      when :down                     then move_cursor_down
      when :"Ctrl+b", :page_up       then move_cursor_page_up(:full)
      when :"Ctrl+f", :page_down     then move_cursor_page_down(:full)
      when :"Ctrl+u"                 then move_cursor_page_up(:half)
      when :"Ctrl+d"                 then move_cursor_page_down(:half)
      when :"Ctrl+left",  :"Alt+b"   then move_cursor_word_left
      when :"Ctrl+right", :"Alt+f"   then move_cursor_word_right
      when :"Ctrl+a", :home          then move_cursor_line_begin
      when :"Ctrl+e", :end           then move_cursor_line_end

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

    def move_cursor_left(set_cursor_id=true)
      if @cursor[0] > 0
        @cursor = [@cursor[0] - 1, @cursor[1]]
      elsif @cursor[1] > 0 || unshift_paragraph
        @cursor[1] -= 1
        @cursor[0] = end_of_line
      end

      @cursor_x = @cursor[0]
      @peer.cursor_id = current_line[@cursor[0]] if set_cursor_id
    end

    def move_cursor_right(set_cursor_id=true)
      if @cursor[0] < end_of_line
        @cursor = [@cursor[0] + 1, @cursor[1]]
      elsif @cursor[1] < num_lines_rendered - 1 || push_paragraph
        @cursor = [0, @cursor[1] + 1]
      end

      @cursor_x = @cursor[0]
      @peer.cursor_id = current_line[@cursor[0]] if set_cursor_id
    end

    def move_cursor_up
      if @cursor[1] > 0 || unshift_paragraph
        @cursor[1] -= 1
        @cursor[0] = [[@cursor[0], @cursor_x || 0].max, end_of_line].min
      else
        @cursor[0] = @cursor_x = 0
      end

      @peer.cursor_id = current_line[@cursor[0]]
    end

    def move_cursor_down
      if @cursor[1] < num_lines_rendered - 1 || push_paragraph
        @cursor[1] += 1
        @cursor[0] = [[@cursor[0], @cursor_x || 0].max, end_of_line].min
      else
        @cursor[0] = @cursor_x = end_of_line
      end

      @peer.cursor_id = current_line[@cursor[0]]
    end

    def move_cursor_page_up(how_much=:full)
      target_scroll = (how_much == :half ? (@canvas_size[1] - 1) / 2 : @canvas_size[1] - 2)

      loop do
        scroll_by = [target_scroll, @scroll_lines].min
        @scroll_lines -= scroll_by
        target_scroll -= scroll_by
        break if target_scroll == 0 || !unshift_paragraph
      end

      if @cursor[1] > @scroll_lines + @canvas_size[1] - 2
        @cursor[1] = @scroll_lines + @canvas_size[1] - 2
        @cursor[0] = [[@cursor[0], @cursor_x || 0].max, end_of_line].min
        @peer.cursor_id = current_line[@cursor[0]]
      end
    end

    def move_cursor_page_down(how_much=:full)
      target_scroll = @scroll_lines + (how_much == :half ? (@canvas_size[1] - 1) / 2 : @canvas_size[1] - 2)
      available_lines = [num_lines_rendered - @scroll_lines - (@canvas_size[1] - 1), 0].max

      loop do
        @scroll_lines += [target_scroll - @scroll_lines, available_lines].min
        break if @scroll_lines == target_scroll || !push_paragraph
        available_lines = @paragraphs.last.num_lines
      end

      if @cursor[1] < @scroll_lines
        @cursor[1] = @scroll_lines
        @cursor[0] = [[@cursor[0], @cursor_x || 0].max, end_of_line].min
        @peer.cursor_id = current_line[@cursor[0]]
      end
    end

    def move_cursor_word_left
      seen_word_char = false
      last_item_id = @peer.cursor_id

      @peer.ordered_list.each_item(@peer.cursor_id, :backwards) do |item|
        next if item.delete_ts
        move_cursor_left(false)
        raise 'ID mismatch' if item.insert_id != current_line[@cursor[0]]

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
        raise 'ID mismatch' if item.insert_id != current_line[@cursor[0]]
        move_cursor_right(false)

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
      @peer.cursor_id = current_line[@cursor[0]]
    end

    def move_cursor_line_end
      @cursor[0] = end_of_line
      @cursor_x = @canvas_size[0]
      @peer.cursor_id = current_line[@cursor[0]]
    end

    def num_lines_rendered
      @paragraphs.inject(0) {|num_lines, para| num_lines + para.num_lines }
    end

    def end_of_line
      current_line.size - 1
    end

    def current_line
      line_no = @cursor[1]
      para_no = 0
      while line_no >= @paragraphs[para_no].num_lines
        line_no -= @paragraphs[para_no].num_lines
        para_no += 1
      end
      @paragraphs[para_no].item_ids[line_no]
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

    # Saves the state of the peer to the filename given in the options.
    def save
      if @options[:text_filename]
        write_safely(@options[:text_filename]) {|file| file.write(peer.ordered_list.to_a.join) }
      end

      if @options[:crdt_filename]
        write_safely(@options[:crdt_filename]) {|file| @peer.save(file) }
      end
    end

    # Writes to a file. To avoid corruption in the case of an inopportune crash or power failure,
    # first writes the data to a temporary file, then renames it to the correct filename.
    def write_safely(filename, &block)
      filename = File.absolute_path(filename)
      temp_fn = File.join(File.dirname(filename), '.' + File.basename(filename) + ".tmp.#{Process.pid}")
      File.open(temp_fn, 'wb') {|file| yield file }
      File.rename(temp_fn, filename)
    end
  end
end
