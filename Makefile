AGE_PUBLIC_KEY=age1e9ccz0qaqey4uyhy3z874p2jsqez5nl9k04cu04j2nuac03aj30shn7v4n

encrypt-secrets:
	@ cat secrets.yaml | age --armor --encrypt -r $(AGE_PUBLIC_KEY) > secrets.yaml.enc
