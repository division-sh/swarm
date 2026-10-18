def phases: ["setup","pairing","claim_restart","confirmation","activation",
  "activation_recovery","enabled_restart","inbox","logout","logout_restart"];
def sites: {
  setup_permissions:["setup"], pairing_claim_readback:["pairing"],
  paired_shutdown:["claim_restart"], prekey_restore:["claim_restart"],
  confirmation_readback:["confirmation"], restored_sdk_connection:["confirmation"],
  confirmation_binding:["confirmation"], confirmation_identity:["confirmation"],
  activation_interruption:["activation"], activation_commit:["activation"], activation_shutdown:["activation"],
  activation_recovery:["activation_recovery"], confirmation_delivery:["activation_recovery"],
  confirmation_delivery_missing:["activation_recovery"], enabled_shutdown:["activation_recovery"],
  enabled_restart_readback:["enabled_restart"], confirmation_resent:["enabled_restart"], reader_open:["enabled_restart"],
  reader_close:["enabled_restart","inbox","logout","logout_restart"],
  inbox_reply_missing:["inbox"], inbox_list_prerequisites:["inbox"], inbox_entry_label:["inbox"],
  inbox_card_correspondence:["inbox"], delivery_read_error:["inbox"], receipt_read_error:["inbox"],
  receipt_missing:["inbox"], inbox_text_mismatch:["inbox"], inbox_operation_mismatch:["inbox"],
  logout_stale_refusal:["logout"], logout_account_refusal:["logout"], logout_settlement:["logout"],
  logout_replay:["logout"], logout_history:["logout"], logout_cli_endpoint:["logout"],
  logout_cli_token_file:["logout"], logout_cli_exit:["logout"], logout_cli_result:["logout"],
  logout_shutdown:["logout"], logout_restart_readback:["logout_restart"],
  logout_history_shutdown:["logout_restart"], logout_restart_connection:["logout_restart"],
  disconnect_timeout:["logout","logout_restart"],
  rpc_result_error:phases, rpc_decode_result:phases, rpc_marshal_request:phases,
  rpc_build_request:phases, rpc_post_request:phases, rpc_http_status:phases, rpc_decode_envelope:phases
};
def flag_names: ["rpc_error","http_ok","awaiting_identity","identity_present","pairing_absent","generation_zero",
  "exit_zero","second_login","prekeys_equal","awaiting_confirmation","runtime_changed","bundle_equal",
  "identity_equal","awaiting_identity_confirmation","external_equal","second_connection","bound","current",
  "conversation_equal","activation_committed","publishing","generation_positive","binding_revision_equal",
  "activation_positive","succeeded","readiness_present","ready","message_operation_equal","text_equal",
  "confirmation_received","generation_equal","unsolicited_send","retired","logout_present","teardown_succeeded",
  "connections_equal","valid","operation_equal","revision_equal","private_target_absent","effect_equal",
  "teardown_equal","phase_equal","parsed","path_exact","query_absent","fragment_absent","reply_received",
  "cursor_absent","unread_zero","label_present","notice","card_matched","sent_candidate","receipt_found","receipt_reference_equal"];
def methods: ["","other","channel.onboarding_start","channel.onboarding_retry","channel.onboarding_get",
  "channel.confirm","channel.logout","mailbox.list"];
def error_classes: ["none","context_canceled","context_deadline","json_syntax","json_type","url","path","unlisted"];
def record_flags($record): if ($record|has("flags")) then $record.flags else {} end;
def valid_leaf:
  type=="string" and test("^TestRunServeWhatsApp((SignedPairing|QuotedCardDecision|AuthoredCustomerReply|AuthoredUncertainReplyRestart|NoticeAcknowledgment|PublicLogout)BothStores/(sqlite|postgres)|PublicResetRetainsPairingBothStores/(retained_source|cleared_source)/(sqlite|postgres))$");
def rejected(reason): {validation:"rejected",reason:reason};
def admitted($event; $record):
  if ($record|type)!="object" then rejected("malformed_record")
  elif ($record.leaf|valid_leaf|not) or $record.leaf!=$event.Test then rejected("invalid_leaf")
  elif (phases|index($record.phase))==null then rejected("invalid_phase")
  elif (error_classes|index($record.error_class))==null then rejected("invalid_error_class")
  elif (methods|index($record.method//""))==null then rejected("invalid_method")
  elif ($record.prior_failure|type)!="boolean" then rejected("invalid_prior_failure")
  elif (record_flags($record)|type)!="object" then rejected("invalid_flags")
  elif any((record_flags($record)|to_entries[]); .key as $key | (flag_names|index($key))!=null and (.value|type)!="boolean") then rejected("invalid_flags")
  elif $record.kind=="phase" and ($record.site//"")=="" then
    {validation:"valid",kind:"phase",test:$event.Test,leaf:$record.leaf,phase:$record.phase,prior_failure:$record.prior_failure}
  elif $record.kind!="site" then rejected("invalid_kind")
  elif ($record.site|type)!="string" then rejected("unknown_site")
  elif sites[$record.site]==null then rejected("unknown_site")
  elif (sites[$record.site]|index($record.phase))==null then rejected("invalid_site_phase")
  else {validation:"valid",kind:"site",test:$event.Test,leaf:$record.leaf,phase:$record.phase,site:$record.site,
    method:($record.method//""),error_class:$record.error_class,
    prior_failure:$record.prior_failure,
    flags:(record_flags($record)|with_entries(.key as $key|select((flag_names|index($key))!=null)))} end;
[inputs | (try fromjson catch null) | select(.Output?!=null) |
  select(.Output|contains("NATIVE_JOURNEY_FAILURE ")) | . as $event |
  ([.Output|capture("NATIVE_JOURNEY_FAILURE (?<data>\\{.*\\})")]) as $matches |
  if ($matches|length)!=1 then rejected("malformed_record")
  else ($matches[0].data | try fromjson catch null) as $record |
    admitted($event;$record) end] | .[]
