package redis

import (
	"context"
	"fmt"
	"lavva/oauth2"
	"lavva/oauth2/models"
	"lavva/oauth2/utils/uuid"
	"time"

	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
)

var (
	_             oauth2.TokenStore = &TokenStore{}
	jsonMarshal                     = jsoniter.Marshal
	jsonUnmarshal                   = jsoniter.Unmarshal
)

// NewRedisStore create an instance of a redis store
func NewRedisStore(opts *redis.Options, keyNamespace ...string) *TokenStore {
	if opts == nil {
		panic("options cannot be nil")
	}

	return NewRedisStoreWithCli(redis.NewClient(opts), keyNamespace...)
}

// NewRedisStoreWithCli create an instance of a redis store
func NewRedisStoreWithCli(cli *redis.Client, keyNamespace ...string) *TokenStore {
	store := &TokenStore{
		cli: cli,
	}

	if len(keyNamespace) > 0 {
		store.ns = keyNamespace[0]
	}

	return store
}

// NewRedisClusterStore create an instance of a redis cluster store
func NewRedisClusterStore(opts *redis.ClusterOptions, keyNamespace ...string) *TokenStore {
	if opts == nil {
		panic("options cannot be nil")
	}

	return NewRedisClusterStoreWithCli(redis.NewClusterClient(opts), keyNamespace...)
}

// NewRedisClusterStoreWithCli create an instance of a redis cluster store
func NewRedisClusterStoreWithCli(cli *redis.ClusterClient, keyNamespace ...string) *TokenStore {
	store := &TokenStore{
		cli: cli,
	}

	if len(keyNamespace) > 0 {
		store.ns = keyNamespace[0]
	}

	return store
}

type clienter interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Exists(ctx context.Context, key ...string) *redis.IntCmd
	TxPipeline() redis.Pipeliner
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Close() error
}

// TokenStore redis token store
type TokenStore struct {
	cli                clienter
	ns                 string
	getAccessExpiresIn func(info oauth2.TokenInfo) time.Duration
}

// SetGetAccessExpiresInHandler
func (s *TokenStore) SetGetAccessExpiresInHandler(fn func(info oauth2.TokenInfo) time.Duration) {
	s.getAccessExpiresIn = fn
}

// Close close the store
func (s *TokenStore) Close() error {
	return s.cli.Close()
}

// wrapperKey
func (s *TokenStore) wrapperKey(key string) string {
	return fmt.Sprintf("%s%s", s.ns, key)
}

// checkError
func (s *TokenStore) checkError(result redis.Cmder) (bool, error) {
	if err := result.Err(); err != nil {
		if err == redis.Nil {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

// remove
func (s *TokenStore) remove(key string) error {
	result := s.cli.Del(context.Background(), s.wrapperKey(key))
	_, err := s.checkError(result)
	return err
}

// removeToken
func (s *TokenStore) removeToken(tokenString string, isRefresh bool) error {
	basicID, err := s.getBasicID(tokenString)
	if err != nil {
		return err
	} else if basicID == "" {
		return nil
	}

	err = s.remove(tokenString)
	if err != nil {
		return err
	}

	token, err := s.getToken(basicID)
	if err != nil {
		return err
	} else if token == nil {
		return nil
	}

	checkToken := token.GetRefresh()
	if isRefresh {
		checkToken = token.GetAccess()
	}
	iresult := s.cli.Exists(context.Background(), s.wrapperKey(checkToken))
	if err := iresult.Err(); err != nil && err != redis.Nil {
		return err
	} else if iresult.Val() == 0 {
		return s.remove(basicID)
	}

	return nil
}

// parseToken
func (s *TokenStore) parseToken(result *redis.StringCmd) (oauth2.TokenInfo, error) {
	if ok, err := s.checkError(result); err != nil {
		return nil, err
	} else if ok {
		return nil, nil
	}

	buf, err := result.Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var token models.Token
	if err := jsonUnmarshal(buf, &token); err != nil {
		return nil, err
	}
	return &token, nil
}

// getToken
func (s *TokenStore) getToken(key string) (oauth2.TokenInfo, error) {
	result := s.cli.Get(context.Background(), s.wrapperKey(key))
	return s.parseToken(result)
}

// parseBasicID
func (s *TokenStore) parseBasicID(result *redis.StringCmd) (string, error) {
	if ok, err := s.checkError(result); err != nil {
		return "", err
	} else if ok {
		return "", nil
	}
	return result.Val(), nil
}

// getBasicID
func (s *TokenStore) getBasicID(token string) (string, error) {
	result := s.cli.Get(context.Background(), s.wrapperKey(token))
	return s.parseBasicID(result)
}

// Create Create and store the new token information
func (s *TokenStore) Create(info oauth2.TokenInfo) error {
	ct := time.Now()
	jv, err := jsonMarshal(info)
	if err != nil {
		return err
	}

	ctx := context.Background()

	pipe := s.cli.TxPipeline()
	if code := info.GetCode(); code != "" {
		pipe.Set(ctx, s.wrapperKey(code), jv, info.GetCodeExpiresIn())
	} else {
		basicID := uuid.Must(uuid.NewRandom()).String()
		aexp := info.GetAccessExpiresIn()

		if s.getAccessExpiresIn != nil {
			aexp = s.getAccessExpiresIn(info)
		}

		rexp := aexp

		if refresh := info.GetRefresh(); refresh != "" {
			rexp = info.GetRefreshCreateAt().Add(info.GetRefreshExpiresIn()).Sub(ct)
			if aexp.Seconds() > rexp.Seconds() {
				aexp = rexp
			}
			pipe.Set(ctx, s.wrapperKey(refresh), basicID, rexp)
		}

		pipe.Set(ctx, s.wrapperKey(info.GetAccess()), basicID, aexp)
		pipe.Set(ctx, s.wrapperKey(basicID), jv, rexp)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	return nil
}

// RemoveByCode Use the authorization code to delete the token information
func (s *TokenStore) RemoveByCode(code string) error {
	return s.remove(code)
}

// RemoveByAccess Use the access token to delete the token information
func (s *TokenStore) RemoveByAccess(access string) error {
	return s.removeToken(access, false)
}

// RemoveByRefresh Use the refresh token to delete the token information
func (s *TokenStore) RemoveByRefresh(refresh string) error {
	return s.removeToken(refresh, false)
}

// GetByCode Use the authorization code for token information data
func (s *TokenStore) GetByCode(code string) (oauth2.TokenInfo, error) {
	return s.getToken(code)
}

// GetByAccess Use the access token for token information data
func (s *TokenStore) GetByAccess(access string) (oauth2.TokenInfo, error) {
	basicID, err := s.getBasicID(access)
	if err != nil || basicID == "" {
		return nil, err
	}
	return s.getToken(basicID)
}

// GetByRefresh Use the refresh token for token information data
func (s *TokenStore) GetByRefresh(refresh string) (oauth2.TokenInfo, error) {
	basicID, err := s.getBasicID(refresh)
	if err != nil || basicID == "" {
		return nil, err
	}
	return s.getToken(basicID)
}
