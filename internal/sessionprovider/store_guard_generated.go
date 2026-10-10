// Code generated from the exact pinned SDK interface signatures; DO NOT EDIT.
package sessionprovider

import (
	"context"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
	"time"
)

func (g *sdkStores) AddOutgoingEvent(ctx context.Context, chatJID types.JID, id types.MessageID, format string, plaintext []byte) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return errSDKReplayStoreDisabled
}
func (g *sdkStores) ClearBufferedEventPlaintext(ctx context.Context, ciphertextHash [32]byte) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return errSDKDecryptedBufferDisabled
}
func (g *sdkStores) DeleteAllIdentities(ctx context.Context, phone string) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().DeleteAllIdentities(ctx, phone)
}

func (g *sdkStores) DeleteAllSessions(ctx context.Context, phone string) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().DeleteAllSessions(ctx, phone)
}

func (g *sdkStores) DeleteAppStateMutationMACs(ctx context.Context, name string, indexMACs [][]byte) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().DeleteAppStateMutationMACs(ctx, name, indexMACs)
}

func (g *sdkStores) DeleteAppStateVersion(ctx context.Context, name string) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().DeleteAppStateVersion(ctx, name)
}

func (g *sdkStores) DeleteExpiredPrivacyTokens(ctx context.Context, cutoff time.Time) (int64, error) {
	var zero0 int64
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().DeleteExpiredPrivacyTokens(ctx, cutoff)
}

func (g *sdkStores) DeleteIdentity(ctx context.Context, address string) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().DeleteIdentity(ctx, address)
}

func (g *sdkStores) DeleteNCTSalt(ctx context.Context) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().DeleteNCTSalt(ctx)
}

func (g *sdkStores) DeleteOldBufferedHashes(ctx context.Context) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return errSDKDecryptedBufferDisabled
}
func (g *sdkStores) DeleteOldOutgoingEvents(ctx context.Context) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return errSDKReplayStoreDisabled
}
func (g *sdkStores) DeleteSession(ctx context.Context, address string) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().DeleteSession(ctx, address)
}

func (g *sdkStores) GenOnePreKey(ctx context.Context) (*keys.PreKey, error) {
	var zero0 *keys.PreKey
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().GenOnePreKey(ctx)
}

func (g *sdkStores) GetAllAppStateSyncKeys(ctx context.Context) ([]*store.AppStateSyncKey, error) {
	var zero0 []*store.AppStateSyncKey
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().GetAllAppStateSyncKeys(ctx)
}

func (g *sdkStores) GetAllContacts(ctx context.Context) (map[types.JID]types.ContactInfo, error) {
	var zero0 map[types.JID]types.ContactInfo
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().GetAllContacts(ctx)
}

func (g *sdkStores) GetAppStateMutationMAC(ctx context.Context, name string, indexMAC []byte) ([]byte, error) {
	var zero0 []byte
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().GetAppStateMutationMAC(ctx, name, indexMAC)
}

func (g *sdkStores) GetAppStateSyncKey(ctx context.Context, id []byte) (*store.AppStateSyncKey, error) {
	var zero0 *store.AppStateSyncKey
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().GetAppStateSyncKey(ctx, id)
}

func (g *sdkStores) GetAppStateVersion(ctx context.Context, name string) (uint64, [128]byte, error) {
	var zero0 uint64
	var zero1 [128]byte
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, zero1, err
	}
	defer release()
	return g.sessionOwner().GetAppStateVersion(ctx, name)
}

func (g *sdkStores) GetBufferedEvent(ctx context.Context, ciphertextHash [32]byte) (*store.BufferedEvent, error) {
	var zero0 *store.BufferedEvent
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return zero0, errSDKDecryptedBufferDisabled
}
func (g *sdkStores) GetChatSettings(ctx context.Context, chat types.JID) (types.LocalChatSettings, error) {
	var zero0 types.LocalChatSettings
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().GetChatSettings(ctx, chat)
}

func (g *sdkStores) GetContact(ctx context.Context, user types.JID) (types.ContactInfo, error) {
	var zero0 types.ContactInfo
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().GetContact(ctx, user)
}

func (g *sdkStores) GetLIDForPN(ctx context.Context, pn types.JID) (types.JID, error) {
	var zero0 types.JID
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.lids.GetLIDForPN(ctx, pn)
}

func (g *sdkStores) GetLatestAppStateSyncKeyID(ctx context.Context) ([]byte, error) {
	var zero0 []byte
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().GetLatestAppStateSyncKeyID(ctx)
}

func (g *sdkStores) GetManyLIDsForPNs(ctx context.Context, pns []types.JID) (map[types.JID]types.JID, error) {
	var zero0 map[types.JID]types.JID
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.lids.GetManyLIDsForPNs(ctx, pns)
}

func (g *sdkStores) GetManySessions(ctx context.Context, addresses []string) (map[string][]byte, error) {
	var zero0 map[string][]byte
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().GetManySessions(ctx, addresses)
}

func (g *sdkStores) GetMessageSecret(ctx context.Context, chat types.JID, sender types.JID, id types.MessageID) ([]byte, types.JID, error) {
	var zero0 []byte
	var zero1 types.JID
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, zero1, err
	}
	defer release()
	return g.sessionOwner().GetMessageSecret(ctx, chat, sender, id)
}

func (g *sdkStores) GetNCTSalt(ctx context.Context) ([]byte, error) {
	var zero0 []byte
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().GetNCTSalt(ctx)
}

func (g *sdkStores) GetOrGenPreKeys(ctx context.Context, count uint32) ([]*keys.PreKey, error) {
	var zero0 []*keys.PreKey
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().GetOrGenPreKeys(ctx, count)
}

func (g *sdkStores) GetOutgoingEvent(ctx context.Context, chatJID types.JID, altChatJID types.JID, id types.MessageID) (string, []byte, error) {
	var zero0 string
	var zero1 []byte
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, zero1, err
	}
	defer release()
	return zero0, zero1, errSDKReplayStoreDisabled
}
func (g *sdkStores) GetPNForLID(ctx context.Context, lid types.JID) (types.JID, error) {
	var zero0 types.JID
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.lids.GetPNForLID(ctx, lid)
}

func (g *sdkStores) GetPreKey(ctx context.Context, id uint32) (*keys.PreKey, error) {
	var zero0 *keys.PreKey
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().GetPreKey(ctx, id)
}

func (g *sdkStores) GetPrivacyToken(ctx context.Context, user types.JID) (*store.PrivacyToken, error) {
	var zero0 *store.PrivacyToken
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().GetPrivacyToken(ctx, user)
}

func (g *sdkStores) GetSenderKey(ctx context.Context, group string, user string) ([]byte, error) {
	var zero0 []byte
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().GetSenderKey(ctx, group, user)
}

func (g *sdkStores) GetSession(ctx context.Context, address string) ([]byte, error) {
	var zero0 []byte
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().GetSession(ctx, address)
}

func (g *sdkStores) GetWASARootSecretID(ctx context.Context, chat types.JID) (types.MessageID, error) {
	var zero0 types.MessageID
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().GetWASARootSecretID(ctx, chat)
}

func (g *sdkStores) HasSession(ctx context.Context, address string) (bool, error) {
	var zero0 bool
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().HasSession(ctx, address)
}

func (g *sdkStores) IsTrustedIdentity(ctx context.Context, address string, key [32]byte) (bool, error) {
	var zero0 bool
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().IsTrustedIdentity(ctx, address, key)
}

func (g *sdkStores) MarkPreKeysAsUploaded(ctx context.Context, upToID uint32) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().MarkPreKeysAsUploaded(ctx, upToID)
}

func (g *sdkStores) MigratePNToLID(ctx context.Context, pn types.JID, lid types.JID) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().MigratePNToLID(ctx, pn, lid)
}

func (g *sdkStores) PutAllContactNames(ctx context.Context, contacts []store.ContactEntry) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().PutAllContactNames(ctx, contacts)
}

func (g *sdkStores) PutAppStateMutationMACs(ctx context.Context, name string, version uint64, mutations []store.AppStateMutationMAC) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().PutAppStateMutationMACs(ctx, name, version, mutations)
}

func (g *sdkStores) PutAppStateSyncKey(ctx context.Context, id []byte, key store.AppStateSyncKey) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().PutAppStateSyncKey(ctx, id, key)
}

func (g *sdkStores) PutAppStateVersion(ctx context.Context, name string, version uint64, hash [128]byte) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().PutAppStateVersion(ctx, name, version, hash)
}

func (g *sdkStores) PutArchived(ctx context.Context, chat types.JID, archived bool) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().PutArchived(ctx, chat, archived)
}

func (g *sdkStores) PutBufferedEvent(ctx context.Context, ciphertextHash [32]byte, plaintext []byte, serverTimestamp time.Time) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return errSDKDecryptedBufferDisabled
}
func (g *sdkStores) PutBusinessName(ctx context.Context, user types.JID, businessName string) (bool, string, error) {
	var zero0 bool
	var zero1 string
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, zero1, err
	}
	defer release()
	return g.sessionOwner().PutBusinessName(ctx, user, businessName)
}

func (g *sdkStores) PutContactName(ctx context.Context, user types.JID, fullName string, firstName string) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().PutContactName(ctx, user, fullName, firstName)
}

func (g *sdkStores) PutIdentity(ctx context.Context, address string, key [32]byte) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().PutIdentity(ctx, address, key)
}

func (g *sdkStores) PutLIDMapping(ctx context.Context, lid types.JID, jid types.JID) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.lids.PutLIDMapping(ctx, lid, jid)
}

func (g *sdkStores) PutManyLIDMappings(ctx context.Context, mappings []store.LIDMapping) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.lids.PutManyLIDMappings(ctx, mappings)
}

func (g *sdkStores) PutManyRedactedPhones(ctx context.Context, entries []store.RedactedPhoneEntry) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().PutManyRedactedPhones(ctx, entries)
}

func (g *sdkStores) PutManySessions(ctx context.Context, sessions map[string][]byte) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().PutManySessions(ctx, sessions)
}

func (g *sdkStores) PutMessageSecret(ctx context.Context, chat types.JID, sender types.JID, id types.MessageID, secret []byte) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().PutMessageSecret(ctx, chat, sender, id, secret)
}

func (g *sdkStores) PutMessageSecrets(ctx context.Context, inserts []store.MessageSecretInsert) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().PutMessageSecrets(ctx, inserts)
}

func (g *sdkStores) PutMutedUntil(ctx context.Context, chat types.JID, mutedUntil time.Time) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().PutMutedUntil(ctx, chat, mutedUntil)
}

func (g *sdkStores) PutNCTSalt(ctx context.Context, salt []byte) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().PutNCTSalt(ctx, salt)
}

func (g *sdkStores) PutPinned(ctx context.Context, chat types.JID, pinned bool) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().PutPinned(ctx, chat, pinned)
}

func (g *sdkStores) PutPrivacyTokens(ctx context.Context, tokens ...store.PrivacyToken) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().PutPrivacyTokens(ctx, tokens...)
}

func (g *sdkStores) PutPushName(ctx context.Context, user types.JID, pushName string) (bool, string, error) {
	var zero0 bool
	var zero1 string
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, zero1, err
	}
	defer release()
	return g.sessionOwner().PutPushName(ctx, user, pushName)
}

func (g *sdkStores) PutSenderKey(ctx context.Context, group string, user string, session []byte) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().PutSenderKey(ctx, group, user, session)
}

func (g *sdkStores) PutSession(ctx context.Context, address string, session []byte) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().PutSession(ctx, address, session)
}

func (g *sdkStores) PutWASARootSecretID(ctx context.Context, chat types.JID, id types.MessageID) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().PutWASARootSecretID(ctx, chat, id)
}

func (g *sdkStores) RemovePreKey(ctx context.Context, id uint32) error {
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	return g.sessionOwner().RemovePreKey(ctx, id)
}

func (g *sdkStores) UploadedPreKeyCount(ctx context.Context) (int, error) {
	var zero0 int
	ctx, release, err := g.fence.acquire(ctx)
	if err != nil {
		return zero0, err
	}
	defer release()
	return g.sessionOwner().UploadedPreKeyCount(ctx)
}
