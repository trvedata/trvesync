require 'crdt'
require 'crdt/editor'

RSpec::Matchers.define :display do |expected|
  match do |screen|
    screen.text.sub(/^[^\n]*\z/, '') == expected.sub('*', '') &&
      screen.cursor == expected_cursor(expected)
  end

  failure_message do |screen|
    "expected cursor #{expected_cursor(expected).inspect} on screen #{expected.sub('*', '').inspect}\n" +
    " but got cursor #{screen.cursor.inspect} on screen #{screen.text.sub(/^[^\n]*\z/, '').inspect}"
  end

  def expected_cursor(text_with_cursor)
    text_with_cursor.split("\n").each_with_index do |line, line_no|
      cursor = line.index('*')
      return [line_no, cursor] if cursor
    end
    nil
  end
end

RSpec.describe CRDT::Editor do
  class MockScreen
    attr_reader :text, :styles, :cursor

    def draw(text, styles, cursor)
      @text, @styles, @cursor = text, styles, cursor
    end

    def should_show(text_with_cursor)
    end
  end

  before :each do
    @editor = CRDT::Editor.new(CRDT::Peer.new, websocket: 'ws://foo.example.com/events')
    @editor.resize(20, 5)
    @screen = MockScreen.new
  end

  def keys(*inputs)
    inputs.each do |input|
      case input
      when String
        input.each_char {|char| @editor.key_pressed(char, @screen) }
      when Array
        input.each {|item| @editor.key_pressed(item, @screen) }
      else
        @editor.key_pressed(input, @screen)
      end
    end
  end

  context 'line wrapping' do
    it 'should wrap lines at word boundaries' do
      keys 'This is my first sentence.'
      expect(@screen).to display "This is my first \nsentence.*\n\n\n"
    end

    it 'should tolerate double spaces' do
      keys 'Please  do  not  write  like  this!'
      expect(@screen).to display "Please  do  not  \nwrite  like  this!*\n\n\n"
    end

    it 'should wrap lines at hypens' do
      keys 'This is a not-quite-so-obvious example.'
      expect(@screen).to display "This is a not-quite-\nso-obvious example.*\n\n\n"
    end

    it 'should break words that are longer than a line' do
      keys 'Try saying Rhabarberbarbarabarbarbarenbartbarbierbierbarbaerbel!'
      expect(@screen).to display "Try saying \nRhabarberbarbarabarb\narbarenbartbarbierbi\nerbarbaerbel!*\n"
    end

    it 'should break lots of whitespace at the screen edge' do
      keys '                              '
      expect(@screen).to display "                    \n          *\n\n\n"
    end

    it 'should move the cursor down on newlines' do
      keys "\n"
      expect(@screen).to display "\n*\n\n\n"
    end

    it 'should break on newlines' do
      keys "First\nand second line (the second wraps)\nNew line"
      expect(@screen).to display "First\nand second line \n(the second wraps)\nNew line*\n"
    end

    it 'should rewrap after deletions' do
      keys 'What is the airspeed velocity of an unladen swallow?', :up, :up
      expect(@screen).to display "What is the \nairspeed* velocity \nof an unladen \nswallow?\n"
      keys [:backspace] * 9
      expect(@screen).to display "What is the* \nvelocity of an \nunladen swallow?\n\n"
    end
  end

  context 'cursor position' do
    it 'should jump to the next line when pressing :right at end of line' do
      keys 'Trying to come up with example text', :up
      expect(@screen.text).to match /\ATrying to come up \nwith example text\n\n\n[^\n]*\z/
      expect { keys :right }.to change { @screen.cursor }.from([0, 17]).to([1, 0])
    end

    it 'should jump to the previous line when pressing :left at beginning of line' do
      keys "Hi there! Isn't this fun?", :home
      expect(@screen.text).to match /\AHi there! Isn't \nthis fun\?\n\n\n[^\n]*\z/
      expect { keys :left }.to change { @screen.cursor }.from([1, 0]).to([0, 15])
    end

    it 'should jump to end of doc when pressing :down on the last line' do
      keys 'Blah blah blah', :home
      expect(@screen.text).to match /\ABlah blah blah\n\n\n\n[^\n]*\z/
      expect { keys :down }.to change { @screen.cursor }.from([0, 0]).to([0, 14])
    end

    it 'should jump to start of doc when pressing :up on the first line' do
      keys "First line\nSecond line", :up
      expect(@screen.text).to match /\AFirst line\nSecond line\n\n\n[^\n]*\z/
      expect { keys :up }.to change { @screen.cursor }.from([0, 10]).to([0, 0])
    end

    it 'should keep the cursor at the document position when rewrapping' do
      keys 'Writing a text editor is fun!', [:left] * 5, ' trem'
      expect(@screen).to display "Writing a text \neditor is trem* fun!\n\n\n"
      keys 'e'
      expect(@screen).to display "Writing a text \neditor is treme* \nfun!\n\n"
      keys 'ndou'
      expect(@screen).to display "Writing a text \neditor is tremendou* \nfun!\n\n"
      keys 's'
      expect(@screen).to display "Writing a text \neditor is \ntremendous* fun!\n\n"
      keys :up, ' really'
      expect(@screen).to display "Writing a text \neditor is really* \ntremendous fun!\n\n"
    end

    it 'should remember x position when moving across short lines' do
      keys "First line is long\nShort\nThird is also long", [:left] * 4
      expect(@screen).to display "First line is long\nShort\nThird is also *long\n\n"
      keys :up
      expect(@screen).to display "First line is long\nShort*\nThird is also long\n\n"
      keys :up
      expect(@screen).to display "First line is *long\nShort\nThird is also long\n\n"
    end

    it 'should set the desired x position when making a change' do
      keys "First line is long\nShorter\nThird is also long", :up, [:backspace] * 2
      expect(@screen).to display "First line is long\nShort*\nThird is also long\n\n"
      keys :up
      expect(@screen).to display "First* line is long\nShort\nThird is also long\n\n"
    end

    it 'should support skipping words backwards' do
      keys 'Hyphen-separated words, punctuation... and more!'
      expect(@screen).to display "Hyphen-separated \nwords, \npunctuation... and \nmore!*\n"
      keys :"Ctrl+left"
      expect(@screen).to display "Hyphen-separated \nwords, \npunctuation... and \n*more!\n"
      keys :"Ctrl+left"
      expect(@screen).to display "Hyphen-separated \nwords, \npunctuation... *and \nmore!\n"
      keys :"Ctrl+left"
      expect(@screen).to display "Hyphen-separated \nwords, \n*punctuation... and \nmore!\n"
      keys [:"Ctrl+left"] * 10
      expect(@screen).to display "*Hyphen-separated \nwords, \npunctuation... and \nmore!\n"
    end

    it 'should support skipping words forwards' do
      keys 'Hyphen-separated words, punctuation... and more!', [:up] * 4
      expect(@screen).to display "*Hyphen-separated \nwords, \npunctuation... and \nmore!\n"
      keys :"Ctrl+right"
      expect(@screen).to display "Hyphen*-separated \nwords, \npunctuation... and \nmore!\n"
      keys :"Ctrl+right"
      expect(@screen).to display "Hyphen-separated* \nwords, \npunctuation... and \nmore!\n"
      keys :"Ctrl+right"
      expect(@screen).to display "Hyphen-separated \nwords*, \npunctuation... and \nmore!\n"
      keys [:"Ctrl+right"] * 10
      expect(@screen).to display "Hyphen-separated \nwords, \npunctuation... and \nmore!*\n"
    end
  end

  context 'scrolling' do
    it 'should show as much as can fit on screen' do
      keys "one\ntwo\nthree\nfour\nfive\nsix\nseven\neight"
      expect(@screen).to display "five\nsix\nseven\neight*\n"
      keys [:up] * 4
      expect(@screen).to display "four*\nfive\nsix\nseven\n"
      keys [:up] * 3
      expect(@screen).to display "one*\ntwo\nthree\nfour\n"
      keys [:down] * 4
      expect(@screen).to display "two\nthree\nfour\nfive*\n"
    end
  end

  context 'editing' do
    it 'should support deleting and inserting at the beginning of the document' do
      keys 'hello world', :home, :delete, 'H'
      expect(@screen).to display "H*ello world\n\n\n\n"
    end

    it 'should support deleting and inserting at the end of the document' do
      keys 'Hello world.', :home, :end, :backspace, '!'
      expect(@screen).to display "Hello world!*\n\n\n\n"
    end
  end
end
