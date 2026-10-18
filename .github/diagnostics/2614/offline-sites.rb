require 'json'
require 'fileutils'
require 'open3'

root = Dir.pwd
out = File.join(root, 'test-results', ENV.fetch('SITE_CHECK_DIR', '2614-sites-offline'))
generated = File.join(ENV.fetch('XDG_CACHE_HOME', File.join(Dir.home, '.cache')), 'agent-g-tools/2614-sites-offline')
FileUtils.mkdir_p(out)
FileUtils.mkdir_p(generated)
source = File.read('internal/serveapp/channel_session_failure_sites_diagnostic_test.go').sub('package serveapp', 'package main')
File.write(File.join(generated, 'capture.go'), source)
FileUtils.cp(File.join(__dir__, 'offline-main.go.txt'), File.join(generated, 'main.go'))
stdout, stderr, status = Open3.capture3('go', 'run', '-race', File.join(generated, 'capture.go'), File.join(generated, 'main.go'))
File.write(File.join(out, 'formatter.log'), stdout+stderr)
raise "formatter failed: #{stderr}" unless status.success?
samples = stdout.lines.map { |line| JSON.parse(line) }
puts 'PASS actual Go formatter/classifier: no Error calls, no prose/path/method leak; race-enabled atomic phase bookkeeping'
plan_path = ENV.fetch('PLAN_PATH', File.join(root, 'test-results/2614-diagnostic-context-fixed-plan/proof-plan.json'))
plan = JSON.parse(File.read(plan_path))
unit = plan.fetch('units').find { |u| u['id'] == 'serveapp-runtime' }
sha = `git rev-parse HEAD`.strip
tree = `git rev-parse HEAD^{tree}`.strip
secret = 'SECRET_FIXTURE_ONLY_NEVER_EXPORT'
cases = samples.reject { |r| r['kind'] == 'phase' }.map { |r| [r['site'], [r], 'SITE_OBSERVED'] }
phase = samples.find { |r| r['kind'] == 'phase' }
cases << ['phase_only', [phase], 'INCOMPLETE']
cases << ['missing_site', [], 'INCOMPLETE']
cases << ['secondary_cleanup', [samples[0].merge('site'=>'reader_close','prior_failure'=>true)], 'INCOMPLETE']
cases << ['unknown_site', [samples[0].merge('site'=>secret)], 'INCOMPLETE']
cases << ['malformed_label', [samples[0].merge('site'=>{'private'=>secret})], 'INCOMPLETE']
cases << ['malformed_json', [:malformed], 'INCOMPLETE']
cases << ['unknown_phase', [samples[0].merge('phase'=>secret)], 'INCOMPLETE']
cases << ['wrong_flag_type', [samples[0].merge('flags'=>{'text_equal'=>secret})], 'INCOMPLETE']
cases << ['false_flags', [samples[0].merge('flags'=>false)], 'INCOMPLETE']
cases << ['secret_payload', [samples[0].merge('message'=>secret,'session'=>secret,'flags'=>samples[0]['flags'].merge(secret=>secret))], 'SITE_OBSERVED']
cases << ['negative', [], 'NEGATIVE']
cases.each do |name, records, expected|
  path = File.join(out, name)
  %w[plan evidence private artifact].each { |dir| FileUtils.mkdir_p(File.join(path, dir)) }
  File.write(File.join(path, 'plan/proof-plan.json'), JSON.generate(plan))
  File.write(File.join(path, 'plan/proof-matrix.json'), '{}')
  File.write(File.join(path, 'plan/identity.json'), JSON.generate({'purpose'=>'DIAGNOSTIC ONLY','qualification_credit'=>false,
    'run_id'=>'123','run_attempt'=>'1','head_sha'=>sha,'tree'=>tree,'base'=>'f2e0d031a06d3f8e5a6ec487c8f90c3e212860d8'}))
  receipt = {'workflow_run_id'=>123,'workflow_attempt'=>1,'plan_digest'=>plan['digest'],'head_sha'=>sha,
    'profile'=>plan['profile'],'build_context'=>plan['build_context'],'unit_id'=>unit['id'],'surface'=>unit['id'],
    'attempt'=>'primary','exit_code'=>(name=='negative' ? 0 : 1),'report'=>{'summary'=>{},'packages'=>[],'tests'=>[]}}
  %w[workload_profile execution_tier packages environment_id count_mode run skip go_timeout].each { |k| receipt[k]=unit[k] if unit.key?(k) }
  File.write(File.join(path, 'evidence/serveapp-runtime-primary-evidence.json'), JSON.generate(receipt))
  leaf = 'TestRunServeWhatsAppPublicLogoutBothStores/sqlite'
  failed = name != 'negative'
  raw = unit['selected_roots'].map { |r| {'Action'=>(failed && r['name']=='TestRunServeWhatsAppPublicLogoutBothStores' ? 'fail' : 'pass'),'Package'=>r['package'],'Test'=>r['name']} }
  raw << {'Action'=>'fail','Package'=>unit['packages'][0],'Test'=>leaf} if failed
  raw << {'Action'=>(failed ? 'fail' : 'pass'),'Package'=>unit['packages'][0]}
  records.each do |record|
    payload = record == :malformed ? '{malformed}' : JSON.generate(record)
    raw << {'Action'=>'output','Package'=>unit['packages'][0],'Test'=>leaf,'Output'=>"NATIVE_JOURNEY_FAILURE #{payload}\n"}
  end
  raw << {'Action'=>'output','Output'=>secret}
  File.write(File.join(path, 'evidence/serveapp-runtime-primary.json'), raw.map { |r| JSON.generate(r) }.join("\n")+"\n")
  File.write(File.join(path, 'private/stdout.log'), secret)
  File.write(File.join(path, 'private/stderr.log'), '')
  File.write(File.join(path, 'private/wrapper-exit.txt'), failed ? "1\n" : "0\n")
  %w[started.txt finished.txt].each { |f| File.write(File.join(path, 'private', f), "2026-10-18T00:00:00Z\n") }
  %w[go-environment.json runner-environment.json].each { |f| File.write(File.join(path, 'artifact', f), '{}') }
  File.write(File.join(path, 'artifact/docker-version.txt'), '28.5.1')
  env = {'GITHUB_SHA'=>sha,'GITHUB_RUN_ID'=>'123','GITHUB_RUN_ATTEMPT'=>'1','RUNNER_TEMP'=>path,
    'DIAGNOSTIC_EVIDENCE_DIR'=>File.join(path,'artifact'),'DIAGNOSTIC_PLAN_DIR'=>File.join(path,'plan'),
    'DIAGNOSTIC_COMMAND_DIR'=>File.join(path,'evidence'),'DIAGNOSTIC_PRIVATE_DIR'=>File.join(path,'private')}
  log, err, result = Open3.capture3(env, 'bash', '.github/diagnostics/2614/collect.sh')
  File.write(File.join(path, 'collector.log'), log+err)
  report = JSON.parse(File.read(File.join(path, 'artifact/result.json')))
  raise "#{name}: #{report} #{err}" unless report['evidence']=='OBSERVED' && report['causal_evidence']==expected && result.success? == (expected!='INCOMPLETE')
  raise 'qualification credit' unless report['qualification_credit'] == false
  Dir[File.join(path, 'artifact/*')].each do |file|
    raise "private data exported in #{file}" if File.file?(file) && File.read(file).include?(secret)
  end
  captured = File.readlines(File.join(path, 'artifact/failure-sites.jsonl')).map { |line| JSON.parse(line) }
  if expected=='SITE_OBSERVED'
    raise 'site lost' unless captured.any? { |r| r['validation']=='valid' && r['site']==records[0]['site'] && r['phase']==records[0]['phase'] }
  elsif name == 'phase_only'
    raise 'phase erased' unless captured[0]['kind']=='phase' && report['failed_leaves_without_site']==[leaf]
  end
  puts "PASS #{name}: transport OBSERVED, causal #{expected}; no private payload exported"
end
