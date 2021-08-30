import os
import shutil
import timeit

start = timeit.default_timer()

CONST_64 = 64
CONST_128 = 128
CONST_192 = 192

REVIEW_DETAILS_LEN = 5
DICTIONARY_DETAILS_LEN = 3

class IndexReader:

    def gapFunction(self, gap_list): # gets strs list of gaps and return the real offset
        new_list = []
        tmp_var = 0
        for i in range(0, len(gap_list)):
            new_list.append(int(gap_list[i]) + tmp_var)
            tmp_var = int(gap_list[i]) + tmp_var
        return new_list

    def readFromBinaryFile(self, file, list_len):
        numbers_list = []

        while True:
            if list_len != 0 and list_len == len(numbers_list):
                return numbers_list
            byte = file.read(1)  # read one byte from file
            if not byte:  # end of rhe file
                return numbers_list
            int_byte = int.from_bytes(byte, 'big')  # convert byte to int
            first_bit = (int_byte >> 7 & 1)  # first bit of byte
            sec_bit = (int_byte >> 6 & 1)  # secound bit of byte

            if (first_bit == 0 and sec_bit == 0):  # case 00
                numbers_list.append(int_byte)

            if (first_bit == 0 and sec_bit == 1):  # case 01
                byte2 = file.read(1)  # read another byte
                byte = (int_byte ^ CONST_64)
                byte = byte.to_bytes(1, 'big')
                numbers_list.append(int.from_bytes(byte + byte2, 'big'))

            if (first_bit == 1 and sec_bit == 0):  # case 10
                byte2 = file.read(2)  # read another byte
                byte = (int_byte ^ CONST_128)
                byte = byte.to_bytes(1, 'big')
                numbers_list.append(int.from_bytes(byte + byte2, 'big'))

            if (first_bit == 1 and sec_bit == 1):  # case 11
                byte2 = file.read(3)  # read another byte
                byte = (int_byte ^ CONST_192)
                byte = byte.to_bytes(1, 'big')
                numbers_list.append(int.from_bytes(byte + byte2, 'big'))

    def  getListByOffset(self, file, setSeek, list_len):
        file.seek(setSeek)  # set the pointer of file to be in the last review
        return self.readFromBinaryFile(file, list_len) # set number of reviews


    def __init__(self, dir):
        """Creates an IndexReader which will read from the given directory"""
        self.dir = dir

        if os.path.isdir(dir):
            # upload the dictionary
            with open(dir + "/indexDictionary.txt", "r") as reader:
                line = reader.readlines()[0].split(' ')
                tokens_keys = line[0::2]
                pointers_values = self.gapFunction(line[1::2])  # fix the gaps
                self.main_dictionary = dict(zip(tokens_keys, pointers_values))  # bulid the tokens and pointes dictionary

            # upload to list the pointers: list[reviewId -1] --> seek to the place of review in reviewDetailsFile
            with open(dir + "/indexReviewOffset.bin", "rb") as reader:
                self.review_id_offset = self.gapFunction(self.readFromBinaryFile(reader, 0)) # convert bin to dec and fix the gaps list

            self.number_of_reviews = len(self.review_id_offset)

    def getProductId(self,reviewId):
        """Returns the product identifier for the given review
        Returns null if there is no review with the given identifier"""
        try:
            with open(self.dir + "/indexProductsIds.txt", "r") as reader:
                for line in reader:
                    list = line.strip('\n').split(' ')  # remove \n

                    if len(list) <= 2 and int(reviewId) == int(list[1]):
                        return list[0]

                    elif len(list) > 2:  # check if list lenght is more than 1
                        new_list = self.gapFunction(list[1:]) # list of rev ids to cuurent product
                        if int(reviewId) in new_list:
                            return list[0]
        except:
            return None

    def getReviewScore(self,reviewId):
        """Returns the score for a given review
        Returns -1 if there is no review with the given identifier"""
        try:
            if self.number_of_reviews >= int(reviewId):
                with open(self.dir + "/indexReviewDetails.bin", "rb") as reader:
                    return self.getListByOffset(reader, self.review_id_offset[int(reviewId)-1], REVIEW_DETAILS_LEN)[3]
            else:
                return -1
        except:
            return -1

    def getReviewHelpfulnessNumerator(self,reviewId):
        """Returns the numerator for the helpfulness of a given review
        Returns -1 if there is no review with the given identifier"""
        try:
            if self.number_of_reviews >= int(reviewId):
                with open(self.dir + "/indexReviewDetails.bin", "rb") as reader:
                    return self.getListByOffset(reader, self.review_id_offset[int(reviewId)-1], REVIEW_DETAILS_LEN)[1]
            else:
                return -1
        except:
            return -1

    def getReviewHelpfulnessDenominator(self,reviewId):
        """Returns the denominator for the helpfulness of a given review
        Returns -1 if there is no review with the given identifier"""
        try:
            if self.number_of_reviews >= int(reviewId):
                with open(self.dir + "/indexReviewDetails.bin", "rb") as reader:
                    return self.getListByOffset(reader, self.review_id_offset[int(reviewId)-1], REVIEW_DETAILS_LEN)[2]
            else:
                return -1
        except:
            return -1

    def getReviewLength(self,reviewId):
        """Returns the number of tokens in a given review
        Returns -1 if there is no review with the given identifier"""
        try:
            if self.number_of_reviews >= int(reviewId) and int(reviewId) > 0:
                with open(self.dir + "/indexReviewDetails.bin", "rb") as reader:
                    return self.getListByOffset(reader, self.review_id_offset[int(reviewId)-1], REVIEW_DETAILS_LEN)[4]
            else:
                return -1
        except:
            return -1

    def getTokenFrequency(self,token):
        """Return the number of reviews containing a given token (i.e., word)
        Returns 0 if there are no reviews containing this token"""
        try:
            seek_to_details = self.main_dictionary[token]
            with open(self.dir + "/indexDictionaryDetails.bin", "rb") as reader:
                return int((self.getListByOffset(reader, seek_to_details, DICTIONARY_DETAILS_LEN)[2])/2)
        except:
            return 0

    def getTokenCollectionFrequency(self,token):
        """Return the number of times that a given token (i.e., word) appears in the reviews indexed
        Returns 0 if there are no reviews containing this token"""
        try:
            seek_to_details = self.main_dictionary[token]
            with open(self.dir + "/indexDictionaryDetails.bin", "rb") as reader:
                return self.getListByOffset(reader, seek_to_details, DICTIONARY_DETAILS_LEN)[0]
        except:
            return 0

    def getReviewsWithToken(self,token):
        """Returns a series of integers of the form id1, freq-1, id-2, freq-2, ...
        such that id-n is the n-th review containing the given token and freq-n is the number
        of times that the token appears in review id-n Note that the integers should be sorted by id
        Returns an empty Tuple if there are no reviews containing this token"""
        try:
            seek_to_details = self.main_dictionary[token]
            # extract the offset to posting list from indexDictionaryDetails file
            with open(self.dir + "/indexDictionaryDetails.bin", "rb") as reader:
                offset = self.getListByOffset(reader, seek_to_details, DICTIONARY_DETAILS_LEN)[1]
                posting_list_len = self.getListByOffset(reader, seek_to_details, DICTIONARY_DETAILS_LEN)[2]

            # extract posting list
            with open(self.dir + "/indexPostingList.bin", "rb") as reader:
                posting_list = self.getListByOffset(reader, offset, posting_list_len)
                id_gaps = self.gapFunction(self.getListByOffset(reader, offset, posting_list_len)[::2]) # fix the gaps of ids in posting list

                # combine between rev ids(accurate ids! not gaps) and freq lists
                result = [None] * (len(posting_list))
                result[0::2] = id_gaps
                result[1::2] = posting_list[1::2]
                return result

        except:
            return []

    def getNumberOfReviews(self):
        """Return the number of product reviews available in the system"""
        try:
            return self.number_of_reviews
        except:
            return 0

    def getTokenSizeOfReviews(self):
        """Return the number of tokens in the system (Tokens should be counted as many times as they appear)"""
        sum = 0
        try:
            with open(self.dir + "/indexReviewDetails.bin", "rb") as reader:
                for i in range(0, self.number_of_reviews):
                    sum+= self.getListByOffset(reader, self.review_id_offset[i], REVIEW_DETAILS_LEN)[4]
            return sum

        except:
            return 0

    def getProductReviews(self,productId):
        """Return the ids of the reviews for a given product identifier Note that the integers
        returned should be sorted by id
        Returns an empty Tuple if there are no reviews for this product"""
        try:
            with open(self.dir + "/indexProductsIds.txt", "r") as reader:
                for line in reader:
                    list = line.strip('\n').split(' ')  # remove \n

                    if list[0] == productId: # check if productId exist
                        if len(list) > 2:  # check if list lenght is more than 1 and
                            return self.gapFunction(list[1:]) # fix the gaps list to be accurate
                        else:
                            return [int(list[1])]
                else: # productId not exist
                    return []
        except:
            return []

    def removeIndex(self, dir):
        if os.path.exists(dir):  # if directory exist remove it
            shutil.rmtree(dir)

r = IndexReader('indexFiles')

print('getProductId')
print(r.getProductId(1)) # B001E4KFG0
print(r.getProductId(14)) # B001GVISJM
print(r.getProductId(23)) # B001GVISJM
print(r.getProductId(50)) # B001EO5QW8
print(r.getProductId(100)) # B0019CW0HE
print(r.getProductId(1001)) # None

print('getReviewScore')
print(r.getReviewScore(2)) # 1
print(r.getReviewScore(37)) # 5
print(r.getReviewScore(100)) # 1
print(r.getReviewScore(1000)) # -1

print('getReviewHelpfulnessNumerator')
print(r.getReviewHelpfulnessNumerator(7)) # 0
print(r.getReviewHelpfulnessNumerator(79)) # 0
print(r.getReviewHelpfulnessNumerator(36)) # 3
print(r.getReviewHelpfulnessNumerator(100)) # 0
print(r.getReviewHelpfulnessNumerator(1000)) # -1

print('getReviewHelpfulnessDenominator')
print(r.getReviewHelpfulnessDenominator(22)) # 0
print(r.getReviewHelpfulnessDenominator(85)) # 4
print(r.getReviewHelpfulnessDenominator(1)) # 1
print(r.getReviewHelpfulnessDenominator(100)) # 1
print(r.getReviewHelpfulnessDenominator(200)) # -1

print('getReviewLength')
print(r.getReviewLength(1)) # 48
print(r.getReviewLength(9)) # 27
print(r.getReviewLength(16)) # 26
print(r.getReviewLength(66)) # 33
print(r.getReviewLength(100)) # 37
print(r.getReviewLength(1000)) # -1
print(r.getReviewLength(9)) # 27
print(r.getReviewLength(-9)) # -1

print('getTokenFrequency')
print(r.getTokenFrequency('i')) # 78
print(r.getTokenFrequency('zip')) # 1
print(r.getTokenFrequency('zoo')) # 0
print(r.getTokenFrequency('year')) # 3
print(r.getTokenFrequency('asdfghj')) # 0

print('getTokenCollectionFrequency')
print(r.getTokenCollectionFrequency('just')) # 21
print(r.getTokenCollectionFrequency('i')) # 227
print(r.getTokenCollectionFrequency('year')) # 4
print(r.getTokenCollectionFrequency('dfghjn')) # 0
print(r.getTokenCollectionFrequency('year')) # 4
print(r.getTokenCollectionFrequency('food')) # 55

print('getReviewsWithToken')
print(r.getReviewsWithToken('0')) # [41, 1]
print(r.getReviewsWithToken('00')) # [43, 1]
print(r.getReviewsWithToken('i')) # [1, 1, 3, 1, 4, 4, 6, 2, 7, 1, 8, 1, 9, 2, 11, 2, 12, 1, 13, 4, 14, 1, 15, 1, 17, 3, 18, 3, 19, 2, 20, 1, 22, 1, 23, 1, 24, 2, 25, 6, 27, 1, 28, 3, 29, 4, 30, 2, 31, 4, 33, 2, 35, 1, 36, 4, 37, 1, 39, 3, 40, 2, 41, 3, 42, 1, 43, 2, 46, 1, 47, 3, 48, 1, 49, 3, 51, 1, 52, 2, 53, 7, 55, 3, 57, 1, 58, 1, 60, 1, 61, 1, 63, 1, 64, 5, 65, 3, 67, 5, 68, 7, 69, 1, 70, 1, 71, 2, 72, 5, 73, 5, 74, 12, 75, 1, 76, 1, 77, 7, 78, 2, 80, 5, 81, 3, 82, 1, 83, 25, 84, 3, 85, 2, 86, 1, 87, 3, 88, 1, 89, 1, 90, 1, 91, 2, 93, 1, 94, 15, 96, 3, 97, 3, 100, 1]
print(r.getReviewsWithToken('just')) # [6, 1, 9, 1, 11, 2, 13, 1, 27, 2, 29, 1, 30, 2, 38, 1, 41, 1, 45, 1, 47, 1, 50, 1, 67, 1, 76, 1, 83, 2, 94, 1, 97, 1]
print(r.getReviewsWithToken('year')) # [74, 1, 93, 1, 97, 2]
print(r.getReviewsWithToken('sdfghbhhh')) # []

print('getNumberOfReviews')
print(r.getNumberOfReviews()) # 100

print('getTokenSizeOfReviews')
print(r.getTokenSizeOfReviews()) # 6903

print('getProductReviews')
print(r.getProductReviews('B001EO5QW8')) # [33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51]
print(r.getProductReviews('B001E4KFG0')) # [1]
print(r.getProductReviews('B001EO5000QW8')) # []
print(r.getProductReviews('B0009XLVG0')) # [12, 13]
print(r.getProductReviews('B001GVISJM')) # [14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28]
print(r.getProductReviews('B0000630MU')) #

end = timeit.default_timer()
print(end - start)