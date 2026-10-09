// Code generated from the exact pinned SDK interface signatures; DO NOT EDIT.
package sessionpersistence

import (
	"context"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
	"time"
)

func (s *sdkStorage) AddOutgoingEvent(ctx context.Context, chatJID types.JID, id types.MessageID, format string, plaintext []byte) error {
	return s.session.AddOutgoingEvent(ctx, chatJID, id, format, plaintext)
}
func (s *sdkStorage) ClearBufferedEventPlaintext(ctx context.Context, ciphertextHash [32]byte) error {
	return s.session.ClearBufferedEventPlaintext(ctx, ciphertextHash)
}
func (s *sdkStorage) DeleteAllIdentities(ctx context.Context, phone string) error {
	return s.session.DeleteAllIdentities(ctx, phone)
}
func (s *sdkStorage) DeleteAllSessions(ctx context.Context, phone string) error {
	return s.session.DeleteAllSessions(ctx, phone)
}
func (s *sdkStorage) DeleteAppStateMutationMACs(ctx context.Context, name string, indexMACs [][]byte) error {
	return s.session.DeleteAppStateMutationMACs(ctx, name, indexMACs)
}
func (s *sdkStorage) DeleteAppStateVersion(ctx context.Context, name string) error {
	return s.session.DeleteAppStateVersion(ctx, name)
}
func (s *sdkStorage) DeleteExpiredPrivacyTokens(ctx context.Context, cutoff time.Time) (int64, error) {
	return s.session.DeleteExpiredPrivacyTokens(ctx, cutoff)
}
func (s *sdkStorage) DeleteIdentity(ctx context.Context, address string) error {
	return s.session.DeleteIdentity(ctx, address)
}
func (s *sdkStorage) DeleteNCTSalt(ctx context.Context) error { return s.session.DeleteNCTSalt(ctx) }
func (s *sdkStorage) DeleteOldBufferedHashes(ctx context.Context) error {
	return s.session.DeleteOldBufferedHashes(ctx)
}
func (s *sdkStorage) DeleteOldOutgoingEvents(ctx context.Context) error {
	return s.session.DeleteOldOutgoingEvents(ctx)
}
func (s *sdkStorage) DeleteSession(ctx context.Context, address string) error {
	return s.session.DeleteSession(ctx, address)
}
func (s *sdkStorage) DoDecryptionTxn(ctx context.Context, fn func(context.Context) error) error {
	return s.session.DoDecryptionTxn(ctx, fn)
}
func (s *sdkStorage) GenOnePreKey(ctx context.Context) (*keys.PreKey, error) {
	return s.session.GenOnePreKey(ctx)
}
func (s *sdkStorage) GetAllAppStateSyncKeys(ctx context.Context) ([]*store.AppStateSyncKey, error) {
	return s.session.GetAllAppStateSyncKeys(ctx)
}
func (s *sdkStorage) GetAllContacts(ctx context.Context) (map[types.JID]types.ContactInfo, error) {
	return s.session.GetAllContacts(ctx)
}
func (s *sdkStorage) GetAppStateMutationMAC(ctx context.Context, name string, indexMAC []byte) ([]byte, error) {
	return s.session.GetAppStateMutationMAC(ctx, name, indexMAC)
}
func (s *sdkStorage) GetAppStateSyncKey(ctx context.Context, id []byte) (*store.AppStateSyncKey, error) {
	return s.session.GetAppStateSyncKey(ctx, id)
}
func (s *sdkStorage) GetAppStateVersion(ctx context.Context, name string) (uint64, [128]byte, error) {
	return s.session.GetAppStateVersion(ctx, name)
}
func (s *sdkStorage) GetBufferedEvent(ctx context.Context, ciphertextHash [32]byte) (*store.BufferedEvent, error) {
	return s.session.GetBufferedEvent(ctx, ciphertextHash)
}
func (s *sdkStorage) GetChatSettings(ctx context.Context, chat types.JID) (types.LocalChatSettings, error) {
	return s.session.GetChatSettings(ctx, chat)
}
func (s *sdkStorage) GetContact(ctx context.Context, user types.JID) (types.ContactInfo, error) {
	return s.session.GetContact(ctx, user)
}
func (s *sdkLIDStorage) GetLIDForPN(ctx context.Context, pn types.JID) (types.JID, error) {
	return s.lids.GetLIDForPN(ctx, pn)
}
func (s *sdkStorage) GetLatestAppStateSyncKeyID(ctx context.Context) ([]byte, error) {
	return s.session.GetLatestAppStateSyncKeyID(ctx)
}
func (s *sdkLIDStorage) GetManyLIDsForPNs(ctx context.Context, pns []types.JID) (map[types.JID]types.JID, error) {
	return s.lids.GetManyLIDsForPNs(ctx, pns)
}
func (s *sdkStorage) GetManySessions(ctx context.Context, addresses []string) (map[string][]byte, error) {
	return s.session.GetManySessions(ctx, addresses)
}
func (s *sdkStorage) GetMessageSecret(ctx context.Context, chat types.JID, sender types.JID, id types.MessageID) ([]byte, types.JID, error) {
	return s.session.GetMessageSecret(ctx, chat, sender, id)
}
func (s *sdkStorage) GetNCTSalt(ctx context.Context) ([]byte, error) {
	return s.session.GetNCTSalt(ctx)
}
func (s *sdkStorage) GetOrGenPreKeys(ctx context.Context, count uint32) ([]*keys.PreKey, error) {
	return s.session.GetOrGenPreKeys(ctx, count)
}
func (s *sdkStorage) GetOutgoingEvent(ctx context.Context, chatJID types.JID, altChatJID types.JID, id types.MessageID) (string, []byte, error) {
	return s.session.GetOutgoingEvent(ctx, chatJID, altChatJID, id)
}
func (s *sdkLIDStorage) GetPNForLID(ctx context.Context, lid types.JID) (types.JID, error) {
	return s.lids.GetPNForLID(ctx, lid)
}
func (s *sdkStorage) GetPreKey(ctx context.Context, id uint32) (*keys.PreKey, error) {
	return s.session.GetPreKey(ctx, id)
}
func (s *sdkStorage) GetPrivacyToken(ctx context.Context, user types.JID) (*store.PrivacyToken, error) {
	return s.session.GetPrivacyToken(ctx, user)
}
func (s *sdkStorage) GetSenderKey(ctx context.Context, group string, user string) ([]byte, error) {
	return s.session.GetSenderKey(ctx, group, user)
}
func (s *sdkStorage) GetSession(ctx context.Context, address string) ([]byte, error) {
	return s.session.GetSession(ctx, address)
}
func (s *sdkStorage) GetWASARootSecretID(ctx context.Context, chat types.JID) (types.MessageID, error) {
	return s.session.GetWASARootSecretID(ctx, chat)
}
func (s *sdkStorage) HasSession(ctx context.Context, address string) (bool, error) {
	return s.session.HasSession(ctx, address)
}
func (s *sdkStorage) IsTrustedIdentity(ctx context.Context, address string, key [32]byte) (bool, error) {
	return s.session.IsTrustedIdentity(ctx, address, key)
}
func (s *sdkStorage) MarkPreKeysAsUploaded(ctx context.Context, upToID uint32) error {
	return s.session.MarkPreKeysAsUploaded(ctx, upToID)
}
func (s *sdkStorage) MigratePNToLID(ctx context.Context, pn types.JID, lid types.JID) error {
	return s.session.MigratePNToLID(ctx, pn, lid)
}
func (s *sdkStorage) PutAllContactNames(ctx context.Context, contacts []store.ContactEntry) error {
	return s.session.PutAllContactNames(ctx, contacts)
}
func (s *sdkStorage) PutAppStateMutationMACs(ctx context.Context, name string, version uint64, mutations []store.AppStateMutationMAC) error {
	return s.session.PutAppStateMutationMACs(ctx, name, version, mutations)
}
func (s *sdkStorage) PutAppStateSyncKey(ctx context.Context, id []byte, key store.AppStateSyncKey) error {
	return s.session.PutAppStateSyncKey(ctx, id, key)
}
func (s *sdkStorage) PutAppStateVersion(ctx context.Context, name string, version uint64, hash [128]byte) error {
	return s.session.PutAppStateVersion(ctx, name, version, hash)
}
func (s *sdkStorage) PutArchived(ctx context.Context, chat types.JID, archived bool) error {
	return s.session.PutArchived(ctx, chat, archived)
}
func (s *sdkStorage) PutBufferedEvent(ctx context.Context, ciphertextHash [32]byte, plaintext []byte, serverTimestamp time.Time) error {
	return s.session.PutBufferedEvent(ctx, ciphertextHash, plaintext, serverTimestamp)
}
func (s *sdkStorage) PutBusinessName(ctx context.Context, user types.JID, businessName string) (bool, string, error) {
	return s.session.PutBusinessName(ctx, user, businessName)
}
func (s *sdkStorage) PutContactName(ctx context.Context, user types.JID, fullName string, firstName string) error {
	return s.session.PutContactName(ctx, user, fullName, firstName)
}
func (s *sdkStorage) PutIdentity(ctx context.Context, address string, key [32]byte) error {
	return s.session.PutIdentity(ctx, address, key)
}
func (s *sdkLIDStorage) PutLIDMapping(ctx context.Context, lid types.JID, jid types.JID) error {
	return s.lids.PutLIDMapping(ctx, lid, jid)
}
func (s *sdkLIDStorage) PutManyLIDMappings(ctx context.Context, mappings []store.LIDMapping) error {
	return s.lids.PutManyLIDMappings(ctx, mappings)
}
func (s *sdkStorage) PutManyRedactedPhones(ctx context.Context, entries []store.RedactedPhoneEntry) error {
	return s.session.PutManyRedactedPhones(ctx, entries)
}
func (s *sdkStorage) PutManySessions(ctx context.Context, sessions map[string][]byte) error {
	return s.session.PutManySessions(ctx, sessions)
}
func (s *sdkStorage) PutMessageSecret(ctx context.Context, chat types.JID, sender types.JID, id types.MessageID, secret []byte) error {
	return s.session.PutMessageSecret(ctx, chat, sender, id, secret)
}
func (s *sdkStorage) PutMessageSecrets(ctx context.Context, inserts []store.MessageSecretInsert) error {
	return s.session.PutMessageSecrets(ctx, inserts)
}
func (s *sdkStorage) PutMutedUntil(ctx context.Context, chat types.JID, mutedUntil time.Time) error {
	return s.session.PutMutedUntil(ctx, chat, mutedUntil)
}
func (s *sdkStorage) PutNCTSalt(ctx context.Context, salt []byte) error {
	return s.session.PutNCTSalt(ctx, salt)
}
func (s *sdkStorage) PutPinned(ctx context.Context, chat types.JID, pinned bool) error {
	return s.session.PutPinned(ctx, chat, pinned)
}
func (s *sdkStorage) PutPrivacyTokens(ctx context.Context, tokens ...store.PrivacyToken) error {
	return s.session.PutPrivacyTokens(ctx, tokens...)
}
func (s *sdkStorage) PutPushName(ctx context.Context, user types.JID, pushName string) (bool, string, error) {
	return s.session.PutPushName(ctx, user, pushName)
}
func (s *sdkStorage) PutSenderKey(ctx context.Context, group string, user string, session []byte) error {
	return s.session.PutSenderKey(ctx, group, user, session)
}
func (s *sdkStorage) PutSession(ctx context.Context, address string, session []byte) error {
	return s.session.PutSession(ctx, address, session)
}
func (s *sdkStorage) PutWASARootSecretID(ctx context.Context, chat types.JID, id types.MessageID) error {
	return s.session.PutWASARootSecretID(ctx, chat, id)
}
func (s *sdkStorage) RemovePreKey(ctx context.Context, id uint32) error {
	return s.session.RemovePreKey(ctx, id)
}
func (s *sdkStorage) UploadedPreKeyCount(ctx context.Context) (int, error) {
	return s.session.UploadedPreKeyCount(ctx)
}
