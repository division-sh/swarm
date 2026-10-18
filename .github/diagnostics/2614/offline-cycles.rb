#!/usr/bin/env ruby
require 'json'
require 'tmpdir'
require 'fileutils'
require 'open3'

root = File.expand_path('../../..', __dir__)
Dir.mktmpdir('2614-cycles-') do |dir|
  parser = File.read(File.join(root, 'internal/apiv1/channel_onboarding_deadlock_cycle_diagnostic.go'))
  File.write(File.join(dir, 'capture.go'), parser.sub('package apiv1', 'package main'))
  FileUtils.cp(File.join(__dir__, 'offline-cycles.go.txt'), File.join(dir, 'main.go'))
  out, err, status = Open3.capture3('go', 'run', '-race', 'capture.go', 'main.go', dir, chdir: dir)
  abort('offline parser validation failed') unless status.success?
  print out
  cycle = JSON.parse(File.read(File.join(dir, 'cycle.json')))
  variants = {
    'cycle' => [cycle, '40P01', 'cycle_observed_owners_unmapped'],
    'unparsed' => [{'state' => 'cycle_unparsed', 'Detail' => 'PRIVATE_CYCLE_PROSE'}, '40P01', 'cycle_unparsed'],
    'bad-mode' => [Marshal.load(Marshal.dump(cycle)), '40P01', 'cycle_unparsed'],
    'bad-label' => [Marshal.load(Marshal.dump(cycle)), '40P01', 'cycle_unparsed'],
    'wrong-state' => [Marshal.load(Marshal.dump(cycle)), '40P01', 'cycle_unparsed'],
    'not-deadlock' => [cycle, '23505', nil]
  }
  variants['bad-mode'][0]['edges'][0]['mode'] = 'PRIVATE_CYCLE_PROSE'
  variants['bad-label'][0]['edges'][0]['waiter'] = 'PRIVATE_CYCLE_PROSE'
  variants['wrong-state'][0]['state'] = 'PRIVATE_CYCLE_PROSE'
  variants.each do |name, (input, state, expected)|
    input['Detail'] = 'PRIVATE_CYCLE_PROSE'
    input['raw_process'] = 402715
    node = {index: 0, parent: -1, depth: 0, type: '*pq.Error', sqlstate: state, deadlock_cycle: input}
    metadata = {request_ordinal: 4, truncated: false, nodes: [node]}
    event = {Time: '2026-10-18T00:00:00Z', Package: 'diagnostic', Test: 'fixture',
             Output: "ONBOARDING_ORIGINAL_CAUSE #{JSON.generate(metadata)}\n"}
    output, _, status = Open3.capture3('jq', '-Rnc', '-f', File.join(__dir__, 'trace.jq'), stdin_data: JSON.generate(event)+"\n")
    abort("collector refused control #{name}") unless status.success?
    got = JSON.parse(output).fetch('cause').fetch('nodes').first['deadlock_cycle']
    abort("collector classification #{name}") unless got&.fetch('state') == expected
    abort("collector privacy #{name}") if output.match?(/PRIVATE_CYCLE_PROSE|402715|raw_process|Detail|Where|InternalQuery/)
    puts "PASS actual collector: #{name} finite edges/unparsed with no prose/native-number export"
  end
end
