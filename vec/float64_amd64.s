// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

#include "textflag.h"
#include "constants.h"

// func matchFloat64EqualAVX2(src []float64, val float64, bits []byte) int64
//
TEXT ·matchFloat64EqualAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $32
	JB		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0
	VMOVDQA		crosslane<>+0x00(SB), Y9
	VMOVDQA		shuffle<>+0x00(SB), Y10

loop_avx2:
	VCMPPD		$0, 0(SI), Y0, Y1
	VCMPPD		$0, 32(SI), Y0, Y2
	VCMPPD		$0, 64(SI), Y0, Y3
	VCMPPD		$0, 96(SI), Y0, Y4
	VCMPPD		$0, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VCMPPD		$0, 160(SI), Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VCMPPD		$0, 192(SI), Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VCMPPD		$0, 224(SI), Y0, Y8
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
	VMOVSD	val+24(FP), X0
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX
	SUBQ	BX, CX

scalar:
	VCOMISD	(SI), X0
	JPS	    scalar_shift
	SETEQ	R10
	ADDL	R10, R9
	ORL	 	R10, AX
scalar_shift:
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


// func matchFloat64NotEqualAVX2(src []float64, val float64, bits []byte) int64
//
TEXT ·matchFloat64NotEqualAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $32
	JB		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0
	VMOVDQA		crosslane<>+0x00(SB), Y9
	VMOVDQA		shuffle<>+0x00(SB), Y10

loop_avx2:
	VCMPPD		$0, 0(SI), Y0, Y1
	VCMPPD		$0, 32(SI), Y0, Y2
	VCMPPD		$0, 64(SI), Y0, Y3
	VCMPPD		$0, 96(SI), Y0, Y4
	VCMPPD		$0, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VCMPPD		$0, 160(SI), Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VCMPPD		$0, 192(SI), Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VCMPPD		$0, 224(SI), Y0, Y8
	VPACKSSDW	Y4, Y8, Y4
	VPERMD		Y4, Y9, Y4
	VPACKSSDW	Y4, Y3, Y3
	VPACKSSWB	Y1, Y3, Y1
	VPSHUFB		Y10, Y1, Y1
	VPMOVMSKB	Y1, AX
	XORQ	    $0xffffffff, AX
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
	VMOVSD	val+24(FP), X0
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX
	SUBQ	BX, CX

scalar:
	VCOMISD	(SI), X0
	JPS	    scalar_shift
	SETEQ	R10
	ADDL	R10, R9
	ORL	 	R10, AX
scalar_shift:
	SHLL	$1, AX
	LEAQ	8(SI), SI
	DECL	BX
	JZ	 	scalar_done
	JMP	 	scalar

scalar_done:
	SHLL	CX, AX
	BSWAPL	AX
	XORQ	$0xffffffff, AX
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

// func matchFloat64LessThanAVX2(src []float64, val float64, bits []byte) int64
//
TEXT ·matchFloat64LessThanAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $32
	JB		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0
	VMOVDQA		crosslane<>+0x00(SB), Y9
	VMOVDQA		shuffle<>+0x00(SB), Y10

loop_avx2:
	VCMPPD		$0x1e, 0(SI), Y0, Y1
	VCMPPD		$0x1e, 32(SI), Y0, Y2
	VCMPPD		$0x1e, 64(SI), Y0, Y3
	VCMPPD		$0x1e, 96(SI), Y0, Y4
	VCMPPD		$0x1e, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VCMPPD		$0x1e, 160(SI), Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VCMPPD		$0x1e, 192(SI), Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VCMPPD		$0x1e, 224(SI), Y0, Y8
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
	VMOVSD	val+24(FP), X0
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX
	SUBQ	BX, CX

scalar:
	VCMPSD  	$0x1e, (SI), X0, X1
	VPMOVMSKB	X1, R10
	ANDL		$1, R10
	ADDL		R10, R9
	ORL	 		R10, AX
	SHLL		$1, AX
	LEAQ		8(SI), SI
	DECL		BX
	JZ	 		scalar_done
	JMP	 		scalar

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

// func matchFloat64LessThanEqualAVX2(src []float64, val float64, bits []byte) int64
//
TEXT ·matchFloat64LessThanEqualAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $32
	JB		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0
	VMOVDQA		crosslane<>+0x00(SB), Y9
	VMOVDQA		shuffle<>+0x00(SB), Y10

loop_avx2:
	VCMPPD		$0x1d, 0(SI), Y0, Y1
	VCMPPD		$0x1d, 32(SI), Y0, Y2
	VCMPPD		$0x1d, 64(SI), Y0, Y3
	VCMPPD		$0x1d, 96(SI), Y0, Y4
	VCMPPD		$0x1d, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VCMPPD		$0x1d, 160(SI), Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VCMPPD		$0x1d, 192(SI), Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VCMPPD		$0x1d, 224(SI), Y0, Y8
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
	VMOVSD	val+24(FP), X0
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX
	SUBQ	BX, CX

scalar:
	VCMPSD  	$0x1d, (SI), X0, X1
	VPMOVMSKB	X1, R10
	ANDL		$1, R10
	ADDL		R10, R9
	ORL	 		R10, AX
	SHLL		$1, AX
	LEAQ		8(SI), SI
	DECL		BX
	JZ	 		scalar_done
	JMP	 		scalar

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


// func matchFloat64GreaterThanAVX2(src []float64, val float64, bits []byte) int64
//
TEXT ·matchFloat64GreaterThanAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $32
	JB		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0
	VMOVDQA		crosslane<>+0x00(SB), Y9
	VMOVDQA		shuffle<>+0x00(SB), Y10

loop_avx2:
	VCMPPD		$0x11, 0(SI), Y0, Y1
	VCMPPD		$0x11, 32(SI), Y0, Y2
	VCMPPD		$0x11, 64(SI), Y0, Y3
	VCMPPD		$0x11, 96(SI), Y0, Y4
	VCMPPD		$0x11, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VCMPPD		$0x11, 160(SI), Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VCMPPD		$0x11, 192(SI), Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VCMPPD		$0x11, 224(SI), Y0, Y8
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
	VMOVSD	val+24(FP), X0
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX
	SUBQ	BX, CX

scalar:
	VCMPSD  	$0x11, (SI), X0, X1
	VPMOVMSKB	X1, R10
	ANDL		$1, R10
	ADDL		R10, R9
	ORL	 		R10, AX
	SHLL		$1, AX
	LEAQ		8(SI), SI
	DECL		BX
	JZ	 		scalar_done
	JMP	 		scalar

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

// func matchFloat64GreaterThanEqualAVX2(src []float64, val float64, bits []byte) int64
//
TEXT ·matchFloat64GreaterThanEqualAVX2(SB), NOSPLIT, $0-64
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+32(FP), DI
	XORQ	R9, R9
	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $32
	JB		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0
	VMOVDQA		crosslane<>+0x00(SB), Y9
	VMOVDQA		shuffle<>+0x00(SB), Y10

loop_avx2:
	VCMPPD		$0x12, 0(SI), Y0, Y1
	VCMPPD		$0x12, 32(SI), Y0, Y2
	VCMPPD		$0x12, 64(SI), Y0, Y3
	VCMPPD		$0x12, 96(SI), Y0, Y4
	VCMPPD		$0x12, 128(SI), Y0, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VCMPPD		$0x12, 160(SI), Y0, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VCMPPD		$0x12, 192(SI), Y0, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VCMPPD		$0x12, 224(SI), Y0, Y8
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
	VMOVSD	val+24(FP), X0
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX
	SUBQ	BX, CX

scalar:
	VCMPSD  	$0x12, (SI), X0, X1
	VPMOVMSKB	X1, R10
	ANDL		$1, R10
	ADDL		R10, R9
	ORL	 		R10, AX
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

// func matchFloat64BetweenAVX2(src []float64, a, b float64, bits []byte) int64
//
TEXT ·matchFloat64BetweenAVX2(SB), NOSPLIT, $0-72
	MOVQ	src_base+0(FP), SI
	MOVQ	src_len+8(FP), BX
	MOVQ	bits_base+40(FP), DI
	XORQ	R9, R9
	TESTQ	BX, BX
	JLE		done
	CMPQ	BX, $32
	JB		prep_scalar

prep_avx2:
	VBROADCASTSD val+24(FP), Y0
	VBROADCASTSD val+32(FP), Y11
	VMOVDQA		crosslane<>+0x00(SB), Y9
	VMOVDQA		shuffle<>+0x00(SB), Y10

loop_avx2:
	VMOVAPD		0(SI), Y1
	VMOVAPD		32(SI), Y2
	VMOVAPD		64(SI), Y3
	VMOVAPD		96(SI), Y4
	VMOVAPD		128(SI), Y5
	VCMPPD		$0x1d, Y0, Y1, Y12
	VCMPPD		$0x1d, Y0, Y2, Y13
	VCMPPD		$0x1d, Y0, Y3, Y14
	VCMPPD		$0x1d, Y0, Y4, Y15
	VCMPPD		$0x12, Y11, Y1, Y1
	VCMPPD		$0x12, Y11, Y2, Y2
	VCMPPD		$0x12, Y11, Y3, Y3
	VCMPPD		$0x12, Y11, Y4, Y4
	VPAND		Y12, Y1, Y1
	VPAND		Y13, Y2, Y2
	VPAND		Y14, Y3, Y3
	VPAND		Y15, Y4, Y4
	VCMPPD		$0x1d, Y0, Y5, Y12
	VCMPPD		$0x12, Y11, Y5, Y5
	VPAND		Y12, Y5, Y5
	VPACKSSDW	Y1, Y5, Y1
	VPERMD		Y1, Y9, Y1
	VMOVAPD		160(SI), Y6
	VCMPPD		$0x1d, Y0, Y6, Y12
	VCMPPD		$0x12, Y11, Y6, Y6
	VPAND		Y12, Y6, Y6
	VPACKSSDW	Y2, Y6, Y2
	VPERMD		Y2, Y9, Y2
	VPACKSSDW	Y2, Y1, Y1
	VMOVAPD		192(SI), Y7
	VCMPPD		$0x1d, Y0, Y7, Y13
	VCMPPD		$0x12, Y11, Y7, Y7
	VPAND		Y13, Y7, Y7
	VPACKSSDW	Y3, Y7, Y3
	VPERMD		Y3, Y9, Y3
	VMOVAPD		224(SI), Y8
	VCMPPD		$0x1d, Y0, Y8, Y14
	VCMPPD		$0x12, Y11, Y8, Y8
	VPAND		Y14, Y8, Y8
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
	VMOVSD	val+24(FP), X0
	VMOVSD	val+32(FP), X1
	XORQ	AX, AX
	XORQ	R10, R10
	MOVQ	BX, R11
	MOVQ	$31, CX
	SUBQ	BX, CX

scalar:
	VMOVSD		(SI), X2
	VCMPSD  	$0x1d, X0, X2, X3
	VCMPSD  	$0x12, X1, X2, X2
	VPAND		X3, X2, X2
	VPMOVMSKB	X2, R10
	ANDL		$1, R10
	ADDL		R10, R9
	ORL	 		R10, AX
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


