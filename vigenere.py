
# (C) Copyright 2018 Heru Arief Wijaya (http://belajararief.com/) untuk INDONESIA.

#a vigenere alphabet 
glbVigenere = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789@?!#$%^&*():"
# glbVigenere = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789@"
 
glbText = "MyPassword@GitHub"
glbKeyword = "GitHub"
 
def Main():
    # print("Text: "+glbText) 
    cipher = Encrypt(glbKeyword, glbText) 
    # print("Encrypted: "+cipher) 
    text = Decrypt(glbKeyword, cipher) 
    # print("Decrypted: "+text)
 
def Encrypt(Keyword, Text): 
    Key = GenerateKey(Keyword, len(Text))
    return GenerateCipher(Key, Text)
 
def Decrypt(Keyword, Text): 
    Key = GenerateKey(Keyword, len(Text))
    return RecoverText(Key, Text)
 
def GenerateKey(keyword, length): 
    rkeyword = keyword#initialize keyword
    while True: #infinate loop
        rkeyword+=keyword 
        if(len(rkeyword)>length): 
            break 
    return rkeyword[:length]#trim if it is too long
 
def GenerateCipher(Key,Text): 
    i = 0 
    Cipher = ''
    while(i<len(Text)):
        Cipher += GetCipherChar(Key[i],Text[i])#generate cipher char
        i += 1 
    return Cipher
 
#!Decryption function
def RecoverText(Key,Cipher): 
    i = 0
    Text = ''
    while(i<len(Cipher)): 
        Text += GetTextChar(Key[i],Cipher[i])#Recover text char
        i += 1
    return Text
 
def GetCipherChar(KeyChar,TextChar):
    shift = MakeVigenereShift(KeyChar)#make a vigenere shift using the keyword char
    TextCharPos = glbVigenere.find(TextChar)#get the position in vigenere of text char
    return shift[TextCharPos]#get the shift char at position of text char (Cipher Char)
 
#!Decryption function
def GetTextChar(KeyChar,CipherChar): 
    shift = MakeVigenereShift(KeyChar)#make a vigenere shift using the keyword char
    CipherCharPos = shift.find(CipherChar)#Get the position of the cipher char in shift
    return glbVigenere[CipherCharPos]#find original text char using cipher char position
 
def MakeVigenereShift(char): 
    pos = glbVigenere.find(char)#get the position of char in vigenere
    return glbVigenere[pos:] + glbVigenere[:pos]#split vigenere (make a shift)
 
Main()