// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants.h"

// func matchInt64EqualAVX2(src []int64, val int64, bits []byte) int64
//
TEXT ·matchInt64EqualAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31
	JBE		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0
	VMOVDQA		crosslane<>+0x00(SB), Y9
	VMOVDQA		shuffle<>+0x00(SB), Y10

loop_avx2:
	VPCMPEQQ	0(SI), Y0, Y1
	VPCMPEQQ	32(SI), Y0, Y2
	VPCMPEQQ	64(SI), Y0, Y3
	VPCMPEQQ	96(SI), Y0, Y4
	VPCMPEQQ	128(SI), Y0, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPCMPEQQ	160(SI), Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VPCMPEQQ	192(SI), Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VPCMPEQQ	224(SI), Y0, Y8
	VPACKSSDW	Y4, Y8, Y4
	VPERMD		Y4, Y9, Y4
	VPACKSSDW	Y4, Y3, Y3
	VPACKSSWB	Y1, Y3, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX
	MOVL		AX, (DI)
	POPCNTQ		AX, AX
	ADDQ		AX, R9
	LEAQ		256(SI), SI
	LEAQ		4(DI), DI
	SUBQ		$32, BX
	CMPQ		BX, $32
	JB		 	exit_avx2
	JMP		 	loop_avx2

exit_avx2:
	VZEROUPPER
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	val+24(FP), DX
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX
	SUBQ	BX, CX

scalar:
	MOVQ	(SI), R8
	CMPQ	R8, DX
	SETEQ	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	SHLL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	SHLL	CX, AX
	BSWAPL	AX
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET

// func matchInt64NotEqualAVX2(src []int64, val int64, bits []byte) int64
//
TEXT ·matchInt64NotEqualAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31
	JBE		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0
	VMOVDQA		crosslane<>+0x00(SB), Y9
	VMOVDQA		shuffle<>+0x00(SB), Y10

loop_avx2:
	VPCMPEQQ	0(SI), Y0, Y1
	VPCMPEQQ	32(SI), Y0, Y2
	VPCMPEQQ	64(SI), Y0, Y3
	VPCMPEQQ	96(SI), Y0, Y4
	VPCMPEQQ	128(SI), Y0, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPCMPEQQ	160(SI), Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VPCMPEQQ	192(SI), Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VPCMPEQQ	224(SI), Y0, Y8
	VPACKSSDW	Y4, Y8, Y4
	VPERMD		Y4, Y9, Y4
	VPACKSSDW	Y4, Y3, Y3
	VPACKSSWB	Y1, Y3, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX

	NOTL        AX
	MOVL		AX, (DI)
	POPCNTQ		AX, AX
	ADDQ		AX, R9
	LEAQ		256(SI), SI
	LEAQ		4(DI), DI
	SUBQ		$32, BX
	CMPQ		BX, $32
	JB		 	exit_avx2
	JMP		 	loop_avx2

exit_avx2:
	VZEROUPPER
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	val+24(FP), DX
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX
	SUBQ	BX, CX

scalar:
	MOVQ	(SI), R8
	CMPQ	R8, DX
	SETNE	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	SHLL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	SHLL	CX, AX
	BSWAPL	AX
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET

// func matchInt64LessThanAVX2(src []int64, val int64, bits []byte) int64
//
TEXT ·matchInt64LessThanAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31
	JBE		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0
	VMOVDQA		crosslane<>+0x00(SB), Y9
	VMOVDQA		shuffle<>+0x00(SB), Y10

loop_avx2:
	VPCMPGTQ	0(SI), Y0, Y1
	VPCMPGTQ	32(SI), Y0, Y2
	VPCMPGTQ	64(SI), Y0, Y3
	VPCMPGTQ	96(SI), Y0, Y4
	VPCMPGTQ	128(SI), Y0, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPCMPGTQ	160(SI), Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VPCMPGTQ	192(SI), Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VPCMPGTQ	224(SI), Y0, Y8
	VPACKSSDW	Y4, Y8, Y4
	VPERMD		Y4, Y9, Y4
	VPACKSSDW	Y4, Y3, Y3
	VPACKSSWB	Y1, Y3, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX
	MOVL		AX, (DI)
	POPCNTQ		AX, AX
	ADDQ		AX, R9
	LEAQ		256(SI), SI
	LEAQ		4(DI), DI
	SUBQ		$32, BX
	CMPQ		BX, $32
	JB		 	exit_avx2
	JMP		 	loop_avx2

exit_avx2:
	VZEROUPPER
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	val+24(FP), DX
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX
	SUBQ	BX, CX

scalar:
	MOVQ	(SI), R8
	CMPQ	R8, DX
	SETLT	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	SHLL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	SHLL	CX, AX
	BSWAPL	AX
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET

// func matchInt64LessThanEqualAVX2(src []int64, val int64, bits []byte) int64
//
TEXT ·matchInt64LessThanEqualAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31
	JBE		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0
	VMOVDQA		crosslane<>+0x00(SB), Y9
	VMOVDQA		shuffle<>+0x00(SB), Y10

loop_avx2:
	VMOVDQA		0(SI), Y1
	VMOVDQA		32(SI), Y2
	VMOVDQA		64(SI), Y3
	VMOVDQA		96(SI), Y4
	VMOVDQA		128(SI), Y5
	VPCMPGTQ	Y0, Y1, Y1
	VPCMPGTQ	Y0, Y2, Y2
	VPCMPGTQ	Y0, Y3, Y3
	VPCMPGTQ	Y0, Y4, Y4
	VPCMPGTQ	Y0, Y5, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VMOVDQA		160(SI), Y6
	VPCMPGTQ	Y0, Y6, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VMOVDQA		192(SI), Y7
	VPCMPGTQ	Y0, Y7, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VMOVDQA		224(SI), Y8
	VPCMPGTQ	Y0, Y8, Y8
	VPACKSSDW	Y4, Y8, Y4
	VPERMD		Y4, Y9, Y4
	VPACKSSDW	Y4, Y3, Y3
	VPACKSSWB	Y1, Y3, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX
	NOTL 		AX
	MOVL		AX, (DI)
	POPCNTQ		AX, AX
	ADDQ		AX, R9
	LEAQ		256(SI), SI
	LEAQ		4(DI), DI
	SUBQ		$32, BX
	CMPQ		BX, $32
	JB		 	exit_avx2
	JMP		 	loop_avx2

exit_avx2:
	VZEROUPPER
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	val+24(FP), DX
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX
	SUBQ	BX, CX

scalar:
	MOVQ	(SI), R8
	CMPQ	R8, DX
	SETLE	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	SHLL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	SHLL	CX, AX
	BSWAPL	AX
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET


// func matchInt64GreaterThanAVX2(src []int64, val int64, bits []byte) int64
//
TEXT ·matchInt64GreaterThanAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31
	JBE		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0
	VMOVDQA		crosslane<>+0x00(SB), Y9
	VMOVDQA		shuffle<>+0x00(SB), Y10

loop_avx2:
	VMOVDQA		0(SI), Y1
	VMOVDQA		32(SI), Y2
	VMOVDQA		64(SI), Y3
	VMOVDQA		96(SI), Y4
	VMOVDQA		128(SI), Y5
	VPCMPGTQ	Y0, Y1, Y1
	VPCMPGTQ	Y0, Y2, Y2
	VPCMPGTQ	Y0, Y3, Y3
	VPCMPGTQ	Y0, Y4, Y4
	VPCMPGTQ	Y0, Y5, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VMOVDQA		160(SI), Y6
	VPCMPGTQ	Y0, Y6, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VMOVDQA		192(SI), Y7
	VPCMPGTQ	Y0, Y7, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VMOVDQA		224(SI), Y8
	VPCMPGTQ	Y0, Y8, Y8
	VPACKSSDW	Y4, Y8, Y4
	VPERMD		Y4, Y9, Y4
	VPACKSSDW	Y4, Y3, Y3
	VPACKSSWB	Y1, Y3, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX
	MOVL		AX, (DI)
	POPCNTQ		AX, AX
	ADDQ		AX, R9
	LEAQ		256(SI), SI
	LEAQ		4(DI), DI
	SUBQ		$32, BX
	CMPQ		BX, $32
	JB		 	exit_avx2
	JMP		 	loop_avx2

exit_avx2:
	VZEROUPPER
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	val+24(FP), DX
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX
	SUBQ	BX, CX

scalar:
	MOVQ	(SI), R8
	CMPQ	R8, DX
	SETGT	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	SHLL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	SHLL	CX, AX
	BSWAPL	AX
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET


// func matchInt64GreaterThanEqualAVX2(src []int64, val int64, bits []byte) int64
//
TEXT ·matchInt64GreaterThanEqualAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31
	JBE		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0
	VMOVDQA		crosslane<>+0x00(SB), Y9
	VMOVDQA		shuffle<>+0x00(SB), Y10

loop_avx2:
	VPCMPGTQ	0(SI), Y0, Y1
	VPCMPGTQ	32(SI), Y0, Y2
	VPCMPGTQ	64(SI), Y0, Y3
	VPCMPGTQ	96(SI), Y0, Y4
	VPCMPGTQ	128(SI), Y0, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VPCMPGTQ	160(SI), Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VPCMPGTQ	192(SI), Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VPCMPGTQ	224(SI), Y0, Y8
	VPACKSSDW	Y4, Y8, Y4
	VPERMD		Y4, Y9, Y4
	VPACKSSDW	Y4, Y3, Y3
	VPACKSSWB	Y1, Y3, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX
	NOTL		AX
	MOVL		AX, (DI)
	POPCNTQ		AX, AX
	ADDQ		AX, R9
	LEAQ		256(SI), SI
	LEAQ		4(DI), DI
	SUBQ		$32, BX
	CMPQ		BX, $32
	JB		 	exit_avx2
	JMP		 	loop_avx2

exit_avx2:
	VZEROUPPER
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	val+24(FP), DX
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX
	SUBQ	BX, CX

scalar:
	MOVQ	(SI), R8
	CMPQ	R8, DX
	SETGE	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	SHLL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	SHLL	CX, AX
	BSWAPL	AX
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+56(FP)
	RET


// func matchInt64BetweenAVX2(src []int64, a, b int64, bits []byte) int64
//
TEXT ·matchInt64BetweenAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+40(FP), DI
	XORQ	R9, R9
	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $31
	JBE		prep_scalar

prep_avx2:
	VPCMPEQQ		Y11, Y11, Y11
	VPSLLQ			$63, Y11, Y11
	VPCMPEQQ		Y13, Y13, Y13
	VPSRLQ			$63, Y13, Y13
	VBROADCASTSD 	val+24(FP), Y12
	VBROADCASTSD 	val+32(FP), Y0
	VPSUBQ			Y12, Y0, Y0
	VPADDQ			Y13, Y0, Y0
	VPXOR			Y11, Y0, Y0
	VMOVDQA			crosslane<>+0x00(SB), Y9
	VMOVDQA			shuffle<>+0x00(SB), Y10

loop_avx2:
	VMOVDQA		0(SI), Y1
	VMOVDQA		32(SI), Y2
	VMOVDQA		64(SI), Y3
	VMOVDQA		96(SI), Y4
	VMOVDQA		128(SI), Y5
	VPSUBQ		Y12, Y1, Y1
	VPSUBQ		Y12, Y2, Y2
	VPSUBQ		Y12, Y3, Y3
	VPSUBQ		Y12, Y4, Y4
	VPSUBQ		Y12, Y5, Y5
	VPXOR		Y11, Y1, Y1
	VPXOR		Y11, Y2, Y2
	VPXOR		Y11, Y3, Y3
	VPXOR		Y11, Y4, Y4
	VPXOR		Y11, Y5, Y5
	VPCMPGTQ	Y1, Y0, Y1
	VPCMPGTQ	Y2, Y0, Y2
	VPCMPGTQ	Y3, Y0, Y3
	VPCMPGTQ	Y4, Y0, Y4
	VPCMPGTQ	Y5, Y0, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VMOVDQA		160(SI), Y6
	VPSUBQ		Y12, Y6, Y6
	VPXOR		Y11, Y6, Y6
	VPCMPGTQ	Y6, Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VMOVDQA		192(SI), Y7
	VPSUBQ		Y12, Y7, Y7
	VPXOR		Y11, Y7, Y7
	VPCMPGTQ	Y7, Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VMOVDQA		224(SI), Y8
	VPSUBQ		Y12, Y8, Y8
	VPXOR		Y11, Y8, Y8
	VPCMPGTQ	Y8, Y0, Y8
	VPACKSSDW	Y4, Y8, Y4
	VPERMD		Y4, Y9, Y4
	VPACKSSDW	Y4, Y3, Y3
	VPACKSSWB	Y1, Y3, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX
	MOVL		AX, (DI)
	POPCNTQ		AX, AX
	ADDQ		AX, R9
	LEAQ		256(SI), SI
	LEAQ		4(DI), DI
	SUBQ		$32, BX
	CMPQ		BX, $32
	JB		 	exit_avx2
	JMP		 	loop_avx2

exit_avx2:
	VZEROUPPER
	TESTQ	BX, BX
	JLE		done

prep_scalar:
	MOVQ	val+24(FP), R13
	MOVQ	val+32(FP), DX
	SUBQ	R13, DX
	INCQ	DX
	MOVQ    $1, R12
	SHLQ    $63, R12
	XORQ    R12, DX
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX
	SUBQ	BX, CX

scalar:
	MOVQ	(SI), R8
	SUBQ	R13, R8
	XORQ    R12, R8
	CMPQ	R8, DX
	SETLT	R10
	ADDL	R10, R9
	ORL	 	R10, AX
	SHLL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	SHLL	CX, AX
	BSWAPL	AX
	CMPQ	R11, $24
	JBE		write_3
	MOVL	AX, (DI)
	JMP		done

write_3:
	CMPQ	R11, $16
	JBE		write_2
	MOVB	AX, (DI)
	SHRL	$8, AX
	INCQ	DI

write_2:
	CMPQ	R11, $8
	JBE		write_1
	MOVW	AX, (DI)
	JMP		done

write_1:
	MOVB	AX, (DI)

done:
	MOVQ	R9, ret+64(FP)
	RET



