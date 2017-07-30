## HadoopOffice security
Security of our solutions is important. Any peer review is very welcome so that organizations using HadoopOffice are never compromised.

### Report security issues
 You can report any security issues to zuinnote@gmail.com. Security issues have depending on their severity over bug fixes and/or work on new features. Please give the HadoopOffice developers and organizations using HadoopOffice some time to respond and we kindly ask you to treat all security issues privately to allow for this.
 
### Credentials to encrypt/decrypt office documents
Currently all credentials in HadoopOffice are passed via Hadoop options. This has two important implications 1) do not make them globally available to all applications 2) HadoopOffice does not store/load credentials. This is task of the application. We strongly recommend to encrypt credentials at rest. Obviously, you should never log credentials.

### Choice of Encryption/Signature Algorithms
HadoopOffice offers depending on the format different algorithms for encryption and signing of documents. Please consult always the documentation on the Wiki and get advice from security experts on which algorithms make sense for your requirements. This may change over time (algorithms get broken etc.) so do not forget to change them if needed.


### Apache POI library depdency
HadoopOffice is tested only with selected version of Apache POI. Using a different version then the one tested will have serious security implications, such as that encryption may not work in old Excel files without any error message.
